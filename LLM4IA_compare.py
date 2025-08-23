import os
import re
import json
import time
import copy
import httpx
import numpy as np
from openai import OpenAI
from collections import OrderedDict
from PostgreSQL import PGHypo as PG
from data_process import DataProcessor
from concurrent.futures import ThreadPoolExecutor

def Prompt_Genaration(w, storage_cost, max_index_length, filter_type, index=None, useless_indexes=None):
    output_str = "Index recommendations are critical to optimizing the performance of workloads in database. Good Indexes can greatly improve the performance of workloads in database. Please complete the index recommendation task according to the following information and requirements.\n"
    rule_str = f"""Rules: There are rules you should follow for index recommendation: 1.Maximize the storage budget to achieve better index optimization, but ensure that the total storage cost of the selected indexes does not exceed the storage budget. 2.Make sure that the tables and columns you use actually exist in the database structure information given. 3. Ensure that all columns that appear in a {max_index_length}-column index belong to the same table.\n"""
    new_columns = dict()
    columns_count_v1 = 0
    columns_count_v2 = 0
    useful_columns = len(w['columns'])
    for k in range(len(list(w['columns'].values()))):
        flag_use = False
        for i in range(0, 12):
            if w['columns'][list(w['columns'].keys())[k]][i] > 0:
                flag_use = True
        if not flag_use:
            useful_columns -= 1
            columns_count_v1 += 1
        else:
            columns_count_v2 += 1
            new_columns[list(w['columns'].keys())[k]] = list(w['columns'].values())[k]
    # 初始化有序字典来存储表和属性
    table_dict = OrderedDict()
    # 遍历给定的字典，按顺序添加属性
    for key in new_columns.keys():
        table_name, attribute = key.split('#')
        if table_name not in table_dict:
            table_dict[table_name] = []  # 使用列表来存储属性
        if attribute not in table_dict[table_name]:
            table_dict[table_name].append(attribute)
    # 输出结果
    table_number = len(list(table_dict.keys()))
    table_kv = dict()
    column_kv = dict()
    table_no = 0
    for table_name in sorted(list(table_dict.keys()), key=len, reverse=True):
        table_dict[table_name] = sorted(table_dict[table_name], key=len, reverse=True)
        table_kv[f't{table_no + 1}'] = table_name
        table_no += 1
    column_number = useful_columns
    for i in range(column_number):
        column_kv[f'c{i + 1}'] = list(new_columns.keys())[i].split("#")[1]
    new_table_rows = dict()
    for i in range(len(list(table_dict.keys()))):
        for j in range(len(list(new_columns.keys()))):
            if list(new_columns.keys())[j].split("#")[0] == list(table_dict.keys())[i]:
                new_table_rows[list(table_dict.keys())[i]] = table_rows[list(new_columns.keys())[j]]
                break
    db_str = f"Given a database with structure (e.g. table(record number)[columns(single column index's storage cost (unit: MB))]) that: "
    for table in list(table_dict.keys()):
        column_str = ''
        for k in range(len(table_dict[table])):
            column_str = column_str + table_dict[table][k] + f'({storage[table + "#" + table_dict[table][k]]}), '
        column_str = column_str[0:len(column_str) - 2]
        db_str += f"{table}({new_table_rows[table]})[{column_str}], "
    db_str = db_str[:len(db_str) - 2] + '.\n'
    if filter_type == 1 or filter_type == 3:
        filters = w['filters1']
    elif filter_type == 2 or filter_type == 4:
        filters = w['filters2']
    elif filter_type == 5:
        filters = w['filters1']
    else:
        raise Exception("filter type error!")
    for key1 in list(filters.keys()):
        if len(list(filters[key1].keys())) > 0:
            rate = 10
            max_num = filters[key1][list(filters[key1].keys())[0]]
            for key2 in list(filters[key1].keys()):
                if filters[key1][key2] * rate < max_num:
                    del filters[key1][key2]
                else:
                    filters[key1][key2] = int(filters[key1][key2])
                    continue
    filter_str = f"Given a filter dictionary of a workload. It contains the overhead of different filter predicates in different operations for each query plan corresponding to that workload: {filters}.\n"
    budget_str = f"Given a storage budget limit: {w['storage budget']}MB.\n"
    prompt_str = f"Chain of Thought: Please think step by step: First, try your bset to recommend a single column index set that will optimize the workload the most within storage budget. Second, for the single-column index set generated in the previous step, try your best to select suitable single-column indexes, and use them as the first column of {max_index_length}-column indexes to extend them into {max_index_length}-column indexes to make the final index set better. Final, give the best recommended index set in a line.\n"
    task_str = f"Task: Please recommend a best index set for the above database, workload and storage budget.\n"
    format_str = "Output Format Requirement: The output should be recommended indexes sets of each step. Each recommended indexes set should be in a line, like: '**Final Recommended Index Set**: [index1;index2;...]'. Each index should be like: table(column) or table(columns). Give each step of the thought process and recommended indexes."
    if filter_type == 1 or filter_type == 2:
        output_str = output_str + filter_str + db_str + budget_str + task_str + prompt_str + rule_str + format_str
    elif filter_type == 3 or filter_type == 4:
        task_str = "Task: Please make improvements to the given set of indexes (i.e., let indexes speed up workload faster) within the given storage budget by: adding a new single-column index or extending an column to an single-column index (e.g., t1(c1)->t1(c1, c2)).\n"
        indexes_str = f"Given a index set: [{index}]. It's storage consumption is: {storage_cost} MB.\n"
        useless_indexes = str(useless_indexes).replace("\'", "")
        not_str = f"Do not use index in: {useless_indexes}. Because they are bad indexes.\n"
        output_str = output_str + filter_str + db_str + budget_str + rule_str + indexes_str + task_str + not_str + format_str
    elif filter_type == 5:
        task_str = "Task: Please make improvements to the given set of indexes (i.e., let indexes speed up workload faster) within the given storage budget by: Look at the characteristics of the different tables and columns in the given database structure,  and try your best to recommend indexes that you think are good and that not in the existing index set.\n"
        indexes_str = f"Given a index set: [{index}]. It's storage consumption is: {storage_cost} MB.\n"
        useless_indexes = str(useless_indexes).replace("\'", "")
        not_str = f"Do not use index in: {useless_indexes}. Because they are bad indexes.\n"
        output_str = output_str + db_str + budget_str + rule_str + indexes_str + task_str + not_str + format_str
    table_kv = dict(sorted(table_kv.items(), key=lambda item: len(item[1]), reverse=True))
    column_kv = dict(sorted(column_kv.items(), key=lambda item: len(item[1]), reverse=True))
    db_structure = str(dict(copy.deepcopy(table_dict)))
    for i in range(column_number):
        db_structure =db_structure.replace(list(column_kv.values())[i], list(column_kv.keys())[i])
        output_str = output_str.replace(list(column_kv.values())[i], list(column_kv.keys())[i])
    for i in range(table_number):
        db_structure = db_structure.replace(list(table_kv.values())[i], list(table_kv.keys())[i])
        output_str = output_str.replace(list(table_kv.values())[i], list(table_kv.keys())[i])
    db_structure = db_structure.replace("#", ".")
    db_structure = db_structure.replace("\'", '\"')
    db_structure = json.loads(db_structure)
    output_str = output_str.replace("#", ".")
    return output_str, table_kv, column_kv, db_structure

def process_requests(api_key, LLM_type, promt_v1=None, prompt_v2=None, num_requests=5, mode=1):
    def single_request(prompt):
        client = OpenAI(
            base_url="https://api.xty.app/v1",
            api_key=api_key,
            http_client=httpx.Client(
                base_url="https://api.xty.app/v1",
                follow_redirects=True,
            ),
        )
        completion = client.chat.completions.create(
            model=LLM_type,
            messages=[
                {"role": "system", "content": "You are an experienced database administrator."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.000001,
        )
        content = completion.choices[0].message.content
        extracted_string = None
        if "**Final Recommended Index Set**" in content:
            content = content.split("**Final Recommended Index Set**")[1]
            pattern = r'\[([^\]]+)\]'
            match = re.search(pattern, content)
            if match:
                extracted_string = match.group(1)
            else:
                print("未找到方括号里的内容")
        elif "**Final Recommended Set**" in content:
            content = content.split("**Final Recommended Set**")[1]
            pattern = r'\[([^\]]+)\]'
            match = re.search(pattern, content)
            if match:
                extracted_string = match.group(1)
            else:
                print("未找到方括号里的内容")
        elif "Finalize Index Recommendations" in content:
            content = content.split("Finalize Index Recommendations")[1]
            pattern = r'\[([^\]]+)\]'
            match = re.search(pattern, content)
            if match:
                extracted_string = match.group(1)
            else:
                print("未找到方括号里的内容")
        elif "### Output" in content:
            content = content.split("### Output")[1]
            pattern = r'\[([^\]]+)\]'
            match = re.search(pattern, content)
            if match:
                extracted_string = match.group(1)
            else:
                print("未找到方括号里的内容")
        elif "Final Index Set" in content:
            content = content.split("Final Index Set")[1]
            pattern = r'\[([^\]]+)\]'
            match = re.search(pattern, content)
            if match:
                extracted_string = match.group(1)
            else:
                print("未找到方括号里的内容")
        elif "Recommended Index Set" in content and "**Final Recommended Index Set**" not in content:
            content = content.split("Recommended Index Set")[1]
            pattern = r'\[([^\]]+)\]'
            matches = re.findall(pattern, content)
            if matches:
                extracted_string = "; ".join(matches)
            else:
                print("未找到方括号里的内容")
        else:
            print(content)
            print("Match Error!")
            time.sleep(10000)
        if extracted_string:
            if "; " not in extracted_string and ";" in extracted_string:
                indexes = extracted_string.split(";")
                new_extracted_string = []
                for index in indexes:
                    if "(" not in index and "." in index:
                        parts = index.split(".")
                        new_index = parts[0] + "(" + parts[1] + ")"
                        new_extracted_string.append(new_index)
                    else:
                        new_extracted_string.append(index)
                extracted_string = "; ".join(new_extracted_string)
        return extracted_string

    def execute_requests(prompt, num_requests):
        results = []
        with ThreadPoolExecutor(max_workers=num_requests) as executor:
            futures = [executor.submit(single_request, prompt) for _ in range(num_requests)]
            for future in futures:
                result = future.result()
                if result:
                    results.append(result)
        return results

    if mode == 1:
        results_v1 = execute_requests(promt_v1, num_requests)
        results_v2 = execute_requests(prompt_v2, num_requests)
        return results_v1 + results_v2
    elif mode == 2:
        return execute_requests(promt_v1, num_requests)

def check_index(db_structure, index):
    index_table = index.split("(")[0]
    try:
        index_columns = index.split("(")[1].split(")")[0]
    except:
        return False
    if ", " in index_columns:
        index_columns = index_columns.split(", ")
        flag_right = True
        for ic in index_columns:
            if ic not in db_structure[index_table]:
                flag_right = False
                break
        if flag_right:
            return True
        else:
            return False
    else:
        if index_columns in db_structure[index_table]:
            return True
        else:
            return False

def rank_indexes(db_connector, indexes, workload):
    db_connector.delete_indexes()
    initial_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    indexes_dict = {}
    for index in indexes:
        oid = db_connector.execute_create_hypo_v3(index)
        storage = db_connector.get_storage_cost(oid)
        current_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
            list(workload.values()))).sum()
        db_connector.execute_delete_hypo(oid)
        reward = (initial_cost - current_cost) / initial_cost / storage
        # reward = (initial_cost - current_cost) * 100 / initial_cost
        indexes_dict[index] = reward
    sorted_indexes = dict(sorted(indexes_dict.items(), key=lambda item: item[1], reverse=True))
    return list(sorted_indexes.keys())

def rank_indexes_v2(db_connector, indexes, workload):
    db_connector.delete_indexes()
    initial_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    indexes_dict = {}
    for index in indexes:
        oid = db_connector.execute_create_hypo_v3(index)
        storage = db_connector.get_storage_cost(oid)
        current_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
            list(workload.values()))).sum()
        db_connector.execute_delete_hypo(oid)
        # reward = (initial_cost - current_cost) / initial_cost / storage
        reward = (initial_cost - current_cost) * 100 / initial_cost
        indexes_dict[index] = reward
    sorted_indexes = dict(sorted(indexes_dict.items(), key=lambda item: item[1], reverse=True))
    return sorted_indexes

def rank_indexes_v3(db_connector, indexes, workload):
    db_connector.delete_indexes()
    initial_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    indexes_dict = {}
    for index in indexes:
        oid = db_connector.execute_create_hypo_v3(index)
        storage = db_connector.get_storage_cost(oid)[0] / 1024 / 1024
        current_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
            list(workload.values()))).sum()
        db_connector.execute_delete_hypo(oid)
        reward = 100 * (initial_cost - current_cost) / initial_cost / storage
        # reward = (initial_cost - current_cost) * 100 / initial_cost
        indexes_dict[index] = reward
    sorted_indexes = dict(sorted(indexes_dict.items(), key=lambda item: item[1], reverse=True))
    return sorted_indexes

def Test_Index(table_kv, column_kv, workload, budget, indexes, useless_indexes):
    table_kv = dict(sorted(table_kv.items(), key=lambda item: len(item[0]), reverse=False))
    column_kv = dict(sorted(column_kv.items(), key=lambda item: len(item[0]), reverse=False))
    for i in range(len(list(column_kv.keys()))):
        indexes = indexes.replace(list(column_kv.keys())[len(list(column_kv.keys())) - i - 1],
                                  list(column_kv.values())[len(list(column_kv.keys())) - i - 1])
    for i in range(len(list(table_kv.keys()))):
        indexes = indexes.replace(list(table_kv.keys())[len(list(table_kv.keys())) - i - 1],
                                  list(table_kv.values())[len(list(table_kv.keys())) - i - 1])
    indexes = indexes.split("; ")
    indexes = rank_indexes(db_connector, indexes, workload)
    db_connector.delete_indexes()
    init_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    storage_sum = 0
    new_indexes = []
    oid_dic = {}
    reward_old = reward_new = 0
    for j in range(len(indexes)):
        oid = db_connector.execute_create_hypo_v3(indexes[j])
        storage = db_connector.get_storage_cost(oid)[0] / 1024 / 1024
        if storage_sum + storage > budget:
            db_connector.execute_delete_hypo(oid)
        else:
            if len(indexes[j].split(", ")) >= 2:
                db_connector.execute_delete_hypo(oid)
                index1 = indexes[j].split(", ")[0] + ")"
                index2 = indexes[j]
                oid1 = db_connector.execute_create_hypo_v3(index1)
                index_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
                    list(workload.values()))).sum()
                reward1 = 100 * (init_cost - index_cost) / init_cost
                db_connector.execute_delete_hypo(oid1)
                oid2 = db_connector.execute_create_hypo_v3(index2)
                index_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
                    list(workload.values()))).sum()
                reward2 = 100 * (init_cost - index_cost) / init_cost
                db_connector.execute_delete_hypo(oid2)
                if reward1 > reward2 - 0.1:
                    if index1 not in useless_indexes:
                        useless_indexes.append(index2)
                    oid = db_connector.execute_create_hypo_v3(index1)
                    storage_new = db_connector.get_storage_cost(oid)[0] / 1024 / 1024
                    if reward1 - 0.1 > reward_old:
                        reward_old = reward_new = reward1
                        if index1 not in new_indexes:
                            storage_sum += storage_new
                            new_indexes.append(index1)
                            oid_dic[index1] = oid
                    else:
                        if index1 not in useless_indexes:
                            useless_indexes.append(index1)
                        db_connector.execute_delete_hypo(oid)
                else:
                    oid = db_connector.execute_create_hypo_v3(index2)
                    if index2 not in new_indexes:
                        storage_sum += storage
                        new_indexes.append(index2)
                        oid_dic[index2] = oid
                    reward_old = reward_new = reward2
            else:
                index_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
                    list(workload.values()))).sum()
                reward_new = 100 * (init_cost - index_cost) / init_cost
                if reward_new - 0.1 > reward_old:
                    reward_old = reward_new
                    if indexes[j] not in new_indexes:
                        storage_sum += storage
                        new_indexes.append(indexes[j])
                        oid_dic[indexes[j]] = oid
                else:
                    if indexes[j] not in useless_indexes:
                        useless_indexes.append(indexes[j])
                    db_connector.execute_delete_hypo(oid)
    new_new_indexes = copy.deepcopy(new_indexes)
    final_indexes = []
    for index1 in new_indexes:
        flag_remove = False
        for index2 in new_new_indexes:
            if len(index1) < len(index2) and index1.split(")")[0] in index2:
                storage = db_connector.get_storage_cost(oid_dic[index1])[0] / 1024 / 1024
                db_connector.execute_delete_hypo(oid_dic[index1])
                index_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
                    list(workload.values()))).sum()
                reward_new = 100 * (init_cost - index_cost) / init_cost
                if reward_new - reward_old < 0.1:
                    reward_old = reward_new
                    storage_sum -= storage
                    flag_remove = True
        if not flag_remove:
            final_indexes.append(index1)
    new_indexes = final_indexes
    print(f"Bugdet: {storage_sum}")
    index_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    db_connector.delete_indexes()
    reward = 100 * (init_cost - index_cost) / init_cost
    for item in new_indexes:
        if item in useless_indexes:
            useless_indexes.remove(item)
    new_indexes_str = "; ".join(new_indexes)
    indexes_str = copy.deepcopy(new_indexes_str)
    for i in range(len(list(column_kv.keys()))):
        new_indexes_str = new_indexes_str.replace(list(column_kv.values())[i], list(column_kv.keys())[i])
    for i in range(len(list(table_kv.keys()))):
        new_indexes_str = new_indexes_str.replace(list(table_kv.values())[i], list(table_kv.keys())[i])
    return storage_sum, reward, new_indexes_str, indexes_str, useless_indexes

def Analysis_Index(db_connector, w, indexes):
    workload = w
    indexes = indexes.split("; ")
    storage_sum = 0
    init_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values())))
    init_cost_sum = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    index_storage = dict()
    for j in range(len(indexes)):
        oid = db_connector.execute_create_hypo_v3(indexes[j])
        storage = db_connector.get_storage_cost(oid)[0] / 1024 / 1024
        index_storage[indexes[j]] = storage
        storage_sum += storage
    index_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values())))
    index_cost_sum = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
        list(workload.values()))).sum()
    return init_cost, init_cost_sum, index_cost, index_cost_sum, storage_sum, index_storage, rank_indexes_v2(db_connector, indexes, workload), rank_indexes_v3(db_connector, indexes, workload)

if __name__ == '__main__':
    # load config file
    config_db = json.load(open(os.getcwd() + "/config_db.json"))
    dataset = config_db["db_name"].split("1")[0]
    db_connector = PG(config_db)
    tables = db_connector.get_tables()
    columns = list()
    for table in tables:
        columns += db_connector.get_columns(table)
    columns = sorted(columns, key=lambda x: x)
    config_LLM = json.load(open(os.getcwd() + "/config_LLM.json"))
    api_key = config_LLM['api_key']

    # index gen
    start_time = time.time()
    compare_config = json.load(open("config_compare.json"))
    workload = compare_config
    workload["filters1"] = dict()
    workload["filters2"] = dict()
    workload["columns"] = dict()
    data_processor = DataProcessor(config_db)
    workload = data_processor.process_workload(workload)
    LLM_type = compare_config["LLM"]
    epoch = compare_config["epoch"]
    budget = compare_config["storage budget"]
    max_index_length= compare_config["max index length"]
    # 表记录数
    path1 = f"1gb_table_rows_{dataset}.json"
    if os.path.exists(path1):
        table_rows = json.load(open(path1))
    else:
        table_rows = {}
        for column in columns:
            table_rows[column] = db_connector.get_table_rows(column.split("#")[0])
        json.dump(table_rows, open(path1, 'w'))
    # 存储
    path2 = f"1gb_storage_{dataset}.json"
    if os.path.exists(path2):
        storage = json.load(open(path2))
    else:
        storage = {}
        for column in columns:
            storage[column] = db_connector.get_storage(column)
        json.dump(storage, open(path2, 'w'))
    # LLM4IA begin
    indexes_trace = dict()

    # Init Index Set Gen
    print("Init Index Gen")
    promt_v1, table_kv, column_kv, db_structure = Prompt_Genaration(workload, 0, max_index_length, 1)
    promt_v2, table_kv, column_kv, db_structure = Prompt_Genaration(workload, 0, max_index_length, 2)
    # ask LLM
    results = process_requests(api_key, LLM_type, promt_v1, promt_v2, epoch)
    new_results = []
    for result in results:
        result = result.split("; ")
        for r in result:
            if check_index(db_structure, r) and r not in new_results:
                new_results.append(r)
    indexes = "; ".join(new_results)
    # Test Index
    useless_indexes = []
    storage_cost, reward, indexes, indexes_original, useless_indexes = Test_Index(table_kv, column_kv, workload["workload"], budget, indexes, useless_indexes)
    indexes_trace["Init Index Set"] = indexes_original
    print(f"Reward: {reward}")
    print(f"Indexes: {indexes}")
    print(f"Indexes Text: {indexes_original}")
    end_time_1 = time.time()

    # Plan-based Enhancement
    print("Plan-based Enhancement")
    indexes_trace["Plan-based Index Set"] = list()
    plan_based_indexes_trace = list()
    plan_based_indexes_flag_trace = list()
    reward_old = reward
    indexes_old = indexes
    indexes_original_old = indexes_original
    round = 0
    while True:
        round += 1
        print(f"Round {round}")
        promt_v3, table_kv, column_kv, db_structure = Prompt_Genaration(workload, storage_cost, max_index_length, 3, indexes, useless_indexes)
        promt_v4, table_kv, column_kv, db_structure = Prompt_Genaration(workload, storage_cost, max_index_length, 4, indexes, useless_indexes)
        # ask LLM
        results = process_requests(api_key, LLM_type, promt_v3, promt_v4, epoch)
        new_results = []
        indexes_trace_temp = []
        for result in results:
            result = result.split("; ")
            new_result = []
            for r in result:
                if check_index(db_structure, r):
                    if r not in new_result:
                        new_result.append(r)
                    if r not in new_results:
                        new_results.append(r)
            new_result = "; ".join(new_result)
            table_kv = dict(sorted(table_kv.items(), key=lambda item: len(item[0]), reverse=False))
            column_kv = dict(sorted(column_kv.items(), key=lambda item: len(item[0]), reverse=False))
            for i in range(len(list(column_kv.keys()))):
                new_result = new_result.replace(list(column_kv.keys())[len(list(column_kv.keys())) - i - 1],
                                          list(column_kv.values())[len(list(column_kv.keys())) - i - 1])
            for i in range(len(list(table_kv.keys()))):
                new_result = new_result.replace(list(table_kv.keys())[len(list(table_kv.keys())) - i - 1],
                                          list(table_kv.values())[len(list(table_kv.keys())) - i - 1])
            indexes_trace_temp.append(new_result)
        for index in indexes.split("; "):
            if index not in new_results:
                new_results.append(index)
        indexes = "; ".join(new_results)
        # Test Index
        old_indexes = copy.deepcopy(indexes_original)
        storage_cost, reward, indexes, indexes_original, useless_indexes = Test_Index(table_kv, column_kv, workload["workload"],
                                                                        budget, indexes, useless_indexes)
        print(f"Reward: {reward}")
        print(f"Indexes: {indexes}")
        print(f"Indexes Text: {indexes_original}")
        if reward > reward_old:
            old_indexes_temp = old_indexes.split("; ")
            indexes_temp = indexes_original.split("; ")
            indexes_gap = list(set(indexes_temp) - set(old_indexes_temp))
            indexes_flag_trace_temp = []
            for indexes_tt in indexes_trace_temp:
                indexes_temp = indexes_tt.split("; ")
                flag_use = False
                for index1 in indexes_temp:
                    for index2 in indexes_gap:
                        if index1.split(")")[0] in index2:
                            flag_use = True
                if flag_use:
                    indexes_flag_trace_temp.append(True)
                else:
                    indexes_flag_trace_temp.append(False)
            indexes_trace["Plan-based Index Set"].append(indexes_original)
            plan_based_indexes_trace.append(indexes_trace_temp)
            plan_based_indexes_flag_trace.append(indexes_flag_trace_temp)
            reward_old = reward
            indexes_old = indexes
            indexes_original_old = indexes_original
        else:
            reward = reward_old
            indexes = indexes_old
            indexes_original = indexes_original_old
            break
    end_time_2 = time.time()

    # Data-based Enhancement
    print("Data-based Enhancement")
    indexes_trace["Data-based Index Set"] = list()
    data_based_indexes_trace = list()
    data_based_indexes_flag_trace = list()
    round = 0
    while True:
        round += 1
        print(f"Round {round}")
        promt_v5, table_kv, column_kv, db_structure = Prompt_Genaration(workload, storage_cost, max_index_length, 5, indexes, useless_indexes)
        # ask LLM
        results = process_requests(api_key, LLM_type, promt_v5, None, epoch+1, 2)
        new_results = []
        indexes_trace_temp = []
        for result in results:
            result = result.split("; ")
            new_result = []
            for r in result:
                if check_index(db_structure, r):
                    if r not in new_result:
                        new_result.append(r)
                    if r not in new_results:
                        new_results.append(r)
            new_result = "; ".join(new_result)
            table_kv = dict(sorted(table_kv.items(), key=lambda item: len(item[0]), reverse=False))
            column_kv = dict(sorted(column_kv.items(), key=lambda item: len(item[0]), reverse=False))
            for i in range(len(list(column_kv.keys()))):
                new_result = new_result.replace(list(column_kv.keys())[len(list(column_kv.keys())) - i - 1],
                                                list(column_kv.values())[len(list(column_kv.keys())) - i - 1])
            for i in range(len(list(table_kv.keys()))):
                new_result = new_result.replace(list(table_kv.keys())[len(list(table_kv.keys())) - i - 1],
                                                list(table_kv.values())[len(list(table_kv.keys())) - i - 1])
            indexes_trace_temp.append(new_result)
        for index in indexes.split("; "):
            if index not in new_results:
                new_results.append(index)
        indexes = "; ".join(new_results)
        # Test Index
        old_indexes = copy.deepcopy(indexes_original)
        storage_cost, reward, indexes, indexes_original, useless_indexes = Test_Index(table_kv, column_kv, workload["workload"],
                                                                        budget, indexes, useless_indexes)
        print(f"Reward: {reward}")
        print(f"Indexes: {indexes}")
        print(f"Indexes Text: {indexes_original}")
        if reward > reward_old:
            old_indexes_temp = old_indexes.split("; ")
            indexes_temp = indexes_original.split("; ")
            indexes_gap = list(set(indexes_temp) - set(old_indexes_temp))
            indexes_flag_trace_temp = []
            for indexes_tt in indexes_trace_temp:
                indexes_temp = indexes_tt.split("; ")
                flag_use = False
                for index1 in indexes_temp:
                    for index2 in indexes_gap:
                        if index1.split(")")[0] in index2:
                            flag_use = True
                if flag_use:
                    indexes_flag_trace_temp.append(True)
                else:
                    indexes_flag_trace_temp.append(False)
            indexes_trace["Data-based Index Set"].append(indexes_original)
            data_based_indexes_trace.append(indexes_trace_temp)
            data_based_indexes_flag_trace.append(indexes_flag_trace_temp)
            reward_old = reward
            indexes_old = indexes
            indexes_original_old = indexes_original
        else:
            reward = reward_old
            indexes = indexes_old
            indexes_original = indexes_original_old
            break
    print("Success run LLM4IA")
    json.dump(indexes_original, open("index.json", 'w'))