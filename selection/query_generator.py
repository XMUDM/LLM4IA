import logging
import os
import platform
import random
import re
import subprocess
import time
import json
from .workload import Query

class QueryGenerator:
    def __init__(self, benchmark_name, scale_factor, db_connector, query_ids, columns):
        self.scale_factor = scale_factor
        self.benchmark_name = benchmark_name
        self.db_connector = db_connector
        self.queries = []
        # 随机Template采样
        # template_num = random.randint(1, len(query_ids))
        self.query_ids = []
        # All columns in current database/schema
        self.columns = columns
        self.generate()

    def filter_queries(self, query_ids):
        self.queries = [query for query in self.queries if query.nr in query_ids]

    def add_new_query(self, query_id, query_text, frequency):
        if not self.db_connector:
            logging.info("{}:".format(self))
            logging.error("No database connector to validate queries")
            raise Exception("database connector missing")
        query_text = self.db_connector.update_query_text(query_text)
        query = Query(query_id, query_text, frequency=frequency)
        self._validate_query(query)
        self._store_indexable_columns(query)
        self.queries.append(query)

    def _validate_query(self, query):
        try:
            self.db_connector.get_plan(query)
        except Exception as e:
            self.db_connector.rollback()
            logging.error("{}: {}".format(self, e))

    def _store_indexable_columns(self, query):
        for column in self.columns:
            if column.name in query.text:
                query.columns.append(column)

    def _generate_tpch(self):
        logging.info("Generating TPC-H Queries")
        self._run_make()
        # Using default parameters (`-d`)
        queries_string = self._run_command(
            ["./qgen", "-c", "-d", "-s", str(self.scale_factor)], return_output=True
        )
        for query in queries_string.split("-- $ID$")[1:]:
            query_id = query.split("Query (Q")[1].split(")")[0]
            query_text = query.split("-- Approved February 1998")[1].strip()
            query_id = int(query_id)
            if self.query_ids and query_id not in self.query_ids:
                continue
            self.add_new_query(query_id, query_text)
        logging.info("Queries generated")

    def _generate_tpcds(self):
        query_files = [
            open(f"query_files/TPCDS/TPCDS_{file_number}.txt", "r")
            for file_number in range(1, 100)
        ]
        for i in range(len(query_files)):
            queries = query_files[i].readlines()[:1]
            query_id = i + 1
            if self.query_ids and query_id not in self.query_ids:
                continue
            query_text = queries[0]
            query_text = self._update_tpcds_query_text(query_text)
            self.add_new_query(query_id, query_text)

    # This manipulates TPC-DS specific queries to work in more DBMSs
    def _update_tpcds_query_text(self, query_text):
        query_text = query_text.replace(") returns", ") as returns")
        replaced_string = "case when lochierarchy = 0"
        if replaced_string in query_text:
            new_string = re.search(
                r"grouping\(.*\)\+" r"grouping\(.*\) " r"as lochierarchy", query_text
            ).group(0)
            new_string = new_string.replace(" as lochierarchy", "")
            new_string = "case when " + new_string + " = 0"
            query_text = query_text.replace(replaced_string, new_string)
        return query_text

    def _run_make(self):
        if "qgen" not in self._files() and "dsqgen" not in self._files():
            logging.info("Running make in {}".format(self.directory))
            print("Running make in {}".format(self.directory))
            self._run_command(self.make_command)
        else:
            logging.debug("No need to run make")

    def _run_command(self, command, return_output=False, shell=False):
        env = os.environ.copy()
        env["DSS_QUERY"] = "queries"
        p = subprocess.Popen(
            command,
            cwd=self.directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=shell,
            env=env,
        )
        with p.stdout:
            output_string = p.stdout.read().decode("utf-8")
        p.wait()
        if return_output:
            return output_string
        else:
            logging.debug("[SUBPROCESS OUTPUT] " + output_string)

    def _files(self):
        return os.listdir(self.directory)

    def generate(self):
        workload = json.load(open("workload.json"))
        query_id = 1
        for w in list(workload.keys()):
            self.query_ids.append(query_id)
            self.add_new_query(query_id, w, workload[w])
            query_id += 1
