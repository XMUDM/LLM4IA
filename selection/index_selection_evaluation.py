import copy
import json
import logging
import pickle
import sys
import time
import os

from .algorithms.anytime_algorithm import AnytimeAlgorithm
from .algorithms.auto_admin_algorithm import AutoAdminAlgorithm
from .algorithms.db2advis_algorithm import DB2AdvisAlgorithm
from .algorithms.dexter_algorithm import DexterAlgorithm
from .algorithms.drop_heuristic_algorithm import DropHeuristicAlgorithm
from .algorithms.extend_algorithm_storage import ExtendAlgorithm as ExtendAlgorithm_storage
from .algorithms.extend_algorithm_num import ExtendAlgorithm as ExtendAlgorithm_num
from .algorithms.relaxation_algorithm import RelaxationAlgorithm
from .benchmark import Benchmark
from .dbms.hana_dbms import HanaDatabaseConnector
from .dbms.postgres_dbms import PostgresDatabaseConnector
from .query_generator import QueryGenerator
from .selection_algorithm import AllIndexesAlgorithm, NoIndexAlgorithm
from .table_generator import TableGenerator
from .workload import Workload
from .psql.PostgreSQL import PGHypo as PG
from .psql.boo import BagOfOperators

ALGORITHMS = {
    "anytime": AnytimeAlgorithm,
    "auto_admin": AutoAdminAlgorithm,
    "db2advis": DB2AdvisAlgorithm,
    "dexter": DexterAlgorithm,
    "drop": DropHeuristicAlgorithm,
    "extend_num": ExtendAlgorithm_num,
    "extend_storage": ExtendAlgorithm_storage,
    "relaxation": RelaxationAlgorithm,
    "no_index": NoIndexAlgorithm,
    "all_indexes": AllIndexesAlgorithm,
}

DBMSYSTEMS = {"postgres": PostgresDatabaseConnector, "hana": HanaDatabaseConnector}
logging.basicConfig(
    filename='logging.log',
    format = '%(filename)s %(levelname)s %(message)s',
    level=logging.INFO)

class IndexSelection:
    def __init__(self):
        # 设置log级别为DEBUG
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Init IndexSelection")
        # 初始化工具变量
        self.db_connector = None
        self.default_config_file = "config_extend.json"
        self.disable_output_files = False
        self.database_name = None
        self.database_system = None

    def run(self):
        """This is called when running `python3 -m selection`."""
        # 从命令中载入config文件
        config_file = self._parse_command_line_args()
        # 如果命令没有指定config文件，则使用默认config文件
        if not config_file:
            config_file = self.default_config_file
        logging.info("Starting Index Selection Evaluation")
        logging.info("Using config file: {}".format(config_file))

        # 输入config文件，跑算法
        self._run_algorithms(config_file)

    def _setup_config(self, config):
        # 数据库系统名称：【postgres】
        dbms_class = DBMSYSTEMS[config["database_system"]]
        # 生成数据库系统类
        generating_connector = dbms_class(None, autocommit=True)
        # 生成存有table和column信息的类
        table_generator = TableGenerator(
            config["benchmark_name"], config["scale_factor"], generating_connector
        )
        # 数据库名
        self.database_name = table_generator.database_name()
        # 数据库系统名
        self.database_system = config["database_system"]
        # 生成与数据库连接器类
        self.setup_db_connector(self.database_name, self.database_system)

        if "queries" not in config:
            config["queries"] = None
        # 生成query生成器类
        query_generator = QueryGenerator(
            config["benchmark_name"],
            config["scale_factor"],
            self.db_connector,
            config["queries"],
            table_generator.columns,
        )
        # 生成workload类
        self.workload = Workload(query_generator.queries)

        # 是否将workload类存储在pickle文件中
        if "pickle_workload" in config and config["pickle_workload"] is True:
            pickle_filename = (
                f"workload_{config['benchmark_name']}"
                f"_{len(self.workload.queries)}_queries.pickle"
            )
            pickle.dump(self.workload, open(pickle_filename, "wb"))

    def _run_algorithms(self, config_file):
        # 将json格式的config文件载入成字典格式
        with open(config_file) as f:
            config = json.load(f)

        # 配置config并且初始化一些工具类
        self._setup_config(config)
        # 删除所有索引
        self.db_connector.drop_indexes()

        # Set the random seed to obtain deterministic statistics (and cost estimations)
        # because ANALYZE (and alike) use sampling for large tables
        self.db_connector.create_statistics()
        self.db_connector.commit()

        for algorithm_config in config["algorithms"]:
            # CoPhy must be skipped and manually executed via AMPL because it is not
            # integrated yet.
            if algorithm_config["name"] == "cophy":
                continue

            # There are multiple configs if there is a parameter list
            # configured (as a list in the .json file)
            # 将parameters里的列表变成列表长度个config
            configs = self._find_parameter_list(algorithm_config)
            # 跑n个config的算法
            for algorithm_config_unfolded in configs:
                start_time = time.time()
                cfg = algorithm_config_unfolded
                print(cfg)
                indexes, what_if, cost_requests, cache_hits, reward = self._run_algorithm(cfg)
                index_set_str = ""
                for index in indexes:
                    index_str = index.columns[0].table.name + "#" + index.columns[0].name
                    for idx in range(1, len(index.columns)):
                        index_str = index_str + ',' + index.columns[idx].table.name + "#" + index.columns[idx].name
                    index_set_str = index_set_str + ";" + index_str
                index_set_str = index_set_str[1:]
                calculation_time = round(time.time() - start_time, 2)
                # 加工workload格式
                workload = {}
                for i in range(len(self.workload.queries)):
                    query = self.workload.queries[i].text.replace(';', '')
                    if not query[len(query) - 1].isdigit() and not query[len(query) - 1].isalpha():
                        query = query[0:len(query)-2:]
                    query = query.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                    query = ' '.join(query.split())
                    workload[query] = self.workload.queries[i].frequency
                # 把workload， index， reward存入json文件
                path = "tpch10gb_storage_f10000_13_19_t5.json"
                if os.path.exists(path):
                    data_list = json.load(open(path))
                else:
                    data_list = list()
                column_output = {}
                data = dict()
                data["workload"] = workload
                data["budget"] = algorithm_config_unfolded['parameters']['budget_MB']
                data["columns"] = column_output
                data["index"] = index_set_str
                indexes = index_set_str.split(";")
                new_index_set_str = []
                for index in indexes:
                    if "," in index:
                        columns = index.split(",")
                        new_columns = []
                        for column in columns:
                            new_columns.append(column.split("#")[1])
                        new_index = columns[0].split('#')[0] + "(" + ", ".join(new_columns) + ")"
                    else:
                        new_index = f"{index.split('#')[0]}({index.split('#')[1]})"
                    new_index_set_str.append(new_index)
                index_set_str = "; ".join(new_index_set_str)
                json.dump(index_set_str, open("index.json", 'w'))
                json.dump(calculation_time, open("runtime.json", 'w'))
                data["reward"] = reward
                data_list.append(data)
                json.dump(data_list, open(path, 'w'))
                benchmark = Benchmark(
                    self.workload,
                    indexes,
                    self.db_connector,
                    algorithm_config_unfolded,
                    calculation_time,
                    self.disable_output_files,
                    config,
                    cost_requests,
                    cache_hits,
                    what_if,
                )


    # Parameter list example: {"max_indexes": [5, 10, 20]}
    # Creates config for each value
    def _find_parameter_list(self, algorithm_config):
        parameters = algorithm_config["parameters"]
        configs = []
        if parameters:
            # if more than one list --> raise
            self.__check_parameters(parameters)
            for key, value in parameters.items():
                if isinstance(value, list):
                    for i in value:
                        new_config = copy.deepcopy(algorithm_config)
                        new_config["parameters"][key] = i
                        configs.append(new_config)
        if len(configs) == 0:
            configs.append(algorithm_config)
        return configs

    def __check_parameters(self, parameters):
        counter = 0
        for key, value in parameters.items():
            if isinstance(value, list):
                counter += 1
        if counter > 1:
            raise Exception("Too many parameter lists in config")

    def _run_algorithm(self, config):
        # 删除所有已建虚拟索引
        self.db_connector.drop_indexes()
        self.db_connector.commit()
        # 重新设置db连接器
        self.setup_db_connector(self.database_name, self.database_system)

        # 创建相应的算法类
        algorithm = self.create_algorithm_object(config["name"], config["parameters"])
        logging.info(f"Running algorithm {config}")
        # 运行算法
        indexes, reward = algorithm._calculate_best_indexes(self.workload)
        logging.info(f"Indexes found: {indexes}")
        what_if = algorithm.cost_evaluation.what_if

        # 调用what-if代价估计器的次数
        cost_requests = (
            self.db_connector.cost_estimations
            if config["name"] == "db2advis"
            else algorithm.cost_evaluation.cost_requests
        )
        # 访问cache(索引开销数据池)的次数
        cache_hits = (
            0 if config["name"] == "db2advis" else algorithm.cost_evaluation.cache_hits
        )
        return indexes, what_if, cost_requests, cache_hits, reward

    def create_algorithm_object(self, algorithm_name, parameters):
        algorithm = ALGORITHMS[algorithm_name](self.db_connector, parameters)
        return algorithm

    # 载入config文件
    def _parse_command_line_args(self):
        arguments = sys.argv
        if "CRITICAL_LOG" in arguments:
            logging.getLogger().setLevel(logging.CRITICAL)
        if "ERROR_LOG" in arguments:
            logging.getLogger().setLevel(logging.ERROR)
        if "INFO_LOG" in arguments:
            logging.getLogger().setLevel(logging.INFO)
        if "DISABLE_OUTPUT_FILES" in arguments:
            self.disable_output_files = True
        for argument in arguments:
            if ".json" in argument:
                return argument

    def setup_db_connector(self, database_name, database_system):
        if self.db_connector:
            logging.info("Create new database connector (closing old)")
            self.db_connector.close()
        self.db_connector = DBMSYSTEMS[database_system](database_name)
