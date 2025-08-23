import os
import json
import copy
import time
import subprocess
import numpy as np
from PostgreSQL import PGHypo as PG

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
        current_cost = (np.array(db_connector.get_queries_cost(list(workload.keys()))) * np.array(
            list(workload.values()))).sum()
        reward = (initial_cost - current_cost) * 100 / initial_cost
        indexes_dict[index] = reward
        db_connector.execute_delete_hypo(oid)
    db_connector.delete_indexes()
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
        reward = (initial_cost - current_cost) * 100 / initial_cost / storage
        indexes_dict[index] = reward
        db_connector.execute_delete_hypo(oid)
    db_connector.delete_indexes()
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
    return reward, new_indexes_str, indexes_str, useless_indexes

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
    return init_cost.tolist(), init_cost_sum, index_cost.tolist(), index_cost_sum, storage_sum, index_storage, rank_indexes_v2(db_connector, indexes, workload), rank_indexes_v3(db_connector, indexes, workload)

def execute_command(command):
    process = subprocess.Popen(command, shell=True)
    process.wait()

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

    # Test
    compare_config = json.load(open("config_compare.json"))
    workload = compare_config["workload"]
    compare_IA = compare_config["compare IA"]
    max_index_length = compare_config["max index length"]
    storage_budget = compare_config["storage budget"]
    LLM_name = compare_config["LLM"]
    epoch = compare_config["epoch"]
    print("Test Start")
    print("Testing")
    # run the compare IA
    start_time = time.time()
    if compare_IA == "Extend":
        extend_config = json.load(open("config_extend.json"))
        extend_config["algorithms"][0]["parameters"]["budget_MB"] = [storage_budget]
        json.dump(extend_config, open("config_extend.json", 'w'), indent=4)
        json.dump(workload, open("workload.json", 'w'))
        command = "python -m selection config_extend.json"
        execute_command(command)
    if compare_IA == "DB2Advisor":
        db2_config = json.load(open("config_db2.json"))
        db2_config["algorithms"][0]["parameters"]["budget_MB"] = [storage_budget]
        json.dump(db2_config, open("config_db2.json", 'w'), indent=4)
        json.dump(workload, open("workload.json", 'w'))
        command = "python -m selection config_db2.json"
        execute_command(command)
    if compare_IA == "Relaxation":
        relaxation_config = json.load(open("config_relaxation.json"))
        relaxation_config["algorithms"][0]["parameters"]["budget_MB"] = [storage_budget]
        json.dump(relaxation_config, open("config_relaxation.json", 'w'), indent=4)
        json.dump(workload, open("workload.json", 'w'))
        command = "python -m selection config_relaxation.json"
        execute_command(command)
    end_time = time.time()
    inference_time = end_time - start_time
    index = json.load(open("index.json"))
    init_cost, init_cost_sum, index_cost, index_cost_sum, storage_sum, index_storage, index_reward, index_ratio = Analysis_Index(db_connector, workload, index)
    # run LLM4IA
    start_time2 = time.time()
    command = "python LLM4IA_compare.py"
    execute_command(command)
    end_time2 = time.time()
    inference_time2 = end_time2 - start_time2
    index2 = json.load(open("index.json"))
    init_cost2, init_cost_sum2, index_cost2, index_cost_sum2, storage_sum2, index_storage2, index_reward2, index_ratio2 = Analysis_Index(db_connector, workload, index2)
    # write result
    result = dict()
    result["workload"] = workload
    result["compare IA"] = dict()
    result["compare IA"]["name"] = compare_IA
    result["LLM4IA"] = dict()
    result["LLM4IA"]["name"] = "LLM4IA"
    # 1 Index Selection Parameter
    result["compare IA"]["Index Selection Parameter"] = dict()
    result["compare IA"]["Index Selection Parameter"]["max index length"] = max_index_length
    result["compare IA"]["Index Selection Parameter"]["storage budget"] = storage_budget
    result["LLM4IA"]["Index Selection Parameter"] = dict()
    result["LLM4IA"]["Index Selection Parameter"]["max index length"] = max_index_length
    result["LLM4IA"]["Index Selection Parameter"]["storage budget"] = storage_budget
    # 2 Output Index Set
    result["compare IA"]["Output Index Set"] = index
    result["LLM4IA"]["Output Index Set"] = index2
    # 3 Cost Reduction
    result["compare IA"]["Cost Reduction"] = dict()
    result["compare IA"]["Cost Reduction"]["no index"] = init_cost_sum
    result["compare IA"]["Cost Reduction"]["with indexes"] = index_cost_sum
    result["compare IA"]["Cost Reduction"]["cost reduction ratio"] = 100 * (init_cost_sum - index_cost_sum) / init_cost_sum
    result["LLM4IA"]["Cost Reduction"] = dict()
    result["LLM4IA"]["Cost Reduction"]["no index"] = init_cost_sum2
    result["LLM4IA"]["Cost Reduction"]["with indexes"] = index_cost_sum2
    result["LLM4IA"]["Cost Reduction"]["cost reduction ratio"] = 100 * (init_cost_sum2 - index_cost_sum2) / init_cost_sum2
    # 4 Inference Time
    result["compare IA"]["Inference Time"] = inference_time
    result["LLM4IA"]["Inference Time"] = inference_time2
    # 5 Index Analysis
    result["compare IA"]["Index Analysis"] = dict()
    result["compare IA"]["Index Analysis"]["storage"] = index_storage
    result["compare IA"]["Index Analysis"]["reward"] = index_reward
    result["compare IA"]["Index Analysis"]["ratio"] = index_ratio
    result["LLM4IA"]["Index Analysis"] = dict()
    result["LLM4IA"]["Index Analysis"]["storage"] = index_storage2
    result["LLM4IA"]["Index Analysis"]["reward"] = index_reward2
    result["LLM4IA"]["Index Analysis"]["ratio"] = index_ratio2
    # 6 Cost Per Query
    result["compare IA"]["Cost Per Query"] = dict()
    result["compare IA"]["Cost Per Query"]["no index"] = init_cost
    result["compare IA"]["Cost Per Query"]["with indexes"] = index_cost
    result["LLM4IA"]["Cost Per Query"] = dict()
    result["LLM4IA"]["Cost Per Query"]["no index"] = init_cost2
    result["LLM4IA"]["Cost Per Query"]["with indexes"] = index_cost2
    print("Test End")
    json.dump(result, open("result_compare.json", 'w'), indent=4)

