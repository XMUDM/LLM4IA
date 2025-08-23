import math
import json
from collections import OrderedDict
from planTree_v1 import PlanTreeNode_v1
from planTree_v2 import PlanTreeNode_v2
from planTree_v3 import PlanTreeNode_v3
from PostgreSQL import PGHypo as PG


class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.db_connector = PG(config)
        self.selectivity = self.db_connector.get_selectivity(self.db_connector.get_tables())
        self.selectivity = dict(sorted(self.selectivity.items(), key=lambda x: x[0]))
        self.vocab = list(self.selectivity.keys())
        self.table_rows = {}
        tables = self.db_connector.get_tables()
        tables_rows = dict()
        for table in tables:
            tables_rows[table] = self.db_connector.get_table_rows(table)
        for column in list(self.selectivity.keys()):
            table_name = column.split("#")[0]
            self.table_rows[column] = tables_rows[table_name]
        json.dump(self.selectivity, open("selectivity.json", 'w'))
        json.dump(self.table_rows, open("table_rows.json", 'w'))


    def workload2embedding_v1(self, workload):
        # 初始化有序字典来存储表和属性
        table_dict = OrderedDict()
        # 遍历给定的字典，按顺序添加属性
        for key in self.vocab:
            table_name, attribute = key.split('#')
            if table_name not in table_dict:
                table_dict[table_name] = []  # 使用列表来存储属性
            if attribute not in table_dict[table_name]:
                table_dict[table_name].append(attribute)
        for table_name in sorted(list(table_dict.keys()), key=len, reverse=True):
            table_dict[table_name] = sorted(table_dict[table_name], key=len, reverse=True)
        filters = dict()
        filters['Nested Loop'] = dict()
        filters['Merge Join'] = dict()
        filters['Hash Join'] = dict()
        filters['Seq Scan'] = dict()
        filters['Sort'] = dict()
        filters['Group'] = dict()
        # Plan处理
        for w in list(workload.keys()):
            plan = self.db_connector.get_plan(w)
            plan_tree = PlanTreeNode_v1().plan2tree(plan, self.vocab, table_dict)
            frequency = workload[w]
            if len(plan_tree.attributes) > 0:
                # 'Nested Loop': dim-1
                if plan_tree.node_type == 'Nested Loop':
                    for attr in plan_tree.attributes:
                        max_attr_value = 0
                        if " AND " in attr:
                            attr_s = attr.split(" AND ")
                            for a_s in attr_s:
                                attr_value = 1
                                if " = " in a_s:
                                    columns = a_s.split(" = ")
                                elif " <> " in a_s:
                                    columns = a_s.split(" <> ")
                                for c in columns:
                                    if "_" in c and "." in c:
                                        new_c = c.replace(".", "#").replace("::text", "")
                                        attr_value = attr_value * self.table_rows[new_c] * self.selectivity[new_c]
                                if attr_value > max_attr_value:
                                    max_attr_value = attr_value
                        else:
                            max_attr_value = 1
                            columns = attr.split(" = ")
                            for c in columns:
                                if "_" in c and "." in c:
                                    new_c = c.replace(".", "#").replace("::text", "")
                                    max_attr_value = max_attr_value * self.table_rows[new_c] * self.selectivity[new_c]
                        if attr not in list(filters['Nested Loop'].keys()):
                            filters['Nested Loop'][attr] = frequency * plan_tree.cost
                        else:
                            filters['Nested Loop'][attr] += frequency * plan_tree.cost
                # 'Merge Join': dim-1
                if plan_tree.node_type == 'Merge Join':
                    for attr in plan_tree.attributes:
                        max_attr_value = 0
                        if " AND " in attr:
                            attr_s = attr.split(" AND ")
                            for a_s in attr_s:
                                attr_value = 1
                                columns = a_s.split(" = ")
                                for c in columns:
                                    if "_" in c and "." in c:
                                        new_c = c.replace(".", "#").replace("::text", "")
                                        attr_value = attr_value * self.table_rows[new_c] * self.selectivity[new_c]
                                if attr_value > max_attr_value:
                                    max_attr_value = attr_value
                        else:
                            max_attr_value = 1
                            columns = attr.split(" = ")
                            for c in columns:
                                if "_" in c and "." in c:
                                    new_c = c.replace(".", "#").replace("::text", "")
                                    max_attr_value = max_attr_value * self.table_rows[new_c] * self.selectivity[new_c]
                        if attr not in list(filters['Merge Join'].keys()):
                            filters['Merge Join'][attr] = frequency * plan_tree.cost
                        else:
                            filters['Merge Join'][attr] += frequency * plan_tree.cost
                # 'Hash Join': dim-1
                if plan_tree.node_type == 'Hash Join':
                    for attr in plan_tree.attributes:
                        max_attr_value = 0
                        if " AND " in attr:
                            attr_s = attr.split(" AND ")
                            for a_s in attr_s:
                                attr_value = 1
                                columns = a_s.split(" = ")
                                for c in columns:
                                    if "_" in c and "." in c:
                                        new_c = c.replace(".", "#").replace("::text", "")
                                        attr_value = attr_value * self.table_rows[new_c] * self.selectivity[new_c]
                                if attr_value > max_attr_value:
                                    max_attr_value = attr_value
                        else:
                            max_attr_value = 1
                            columns = attr.split(" = ")
                            for c in columns:
                                if "_" in c and "." in c:
                                    new_c = c.replace(".", "#").replace("::text", "")
                                    max_attr_value = max_attr_value * self.table_rows[new_c] * self.selectivity[new_c]
                        if attr not in list(filters['Hash Join'].keys()):
                            filters['Hash Join'][attr] = frequency * plan_tree.cost
                        else:
                            filters['Hash Join'][attr] += frequency * plan_tree.cost
                # 'Seq Scan': dim-3
                if plan_tree.node_type == 'Seq Scan':
                    for attr in plan_tree.attributes:
                        max_attr_value = 0
                        if " AND " in attr:
                            all_attr = []
                            attr_s = attr.split(" AND ")
                            for a_s in attr_s:
                                attr_value = 1
                                new_attr1 = a_s.split(" ")[0]
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == new_attr1:
                                        all_attr.append(a_s.replace(new_attr1, key.replace("#", ".")))
                                        new_attr = key
                                attr_value = attr_value * self.table_rows[new_attr] * self.selectivity[new_attr]
                                if attr_value > max_attr_value:
                                    max_attr_value = attr_value
                            all_attr = " AND ".join(all_attr)
                        else:
                            max_attr_value = 1
                            new_attr1 = attr.split(" ")[0]
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == new_attr1:
                                    new_attr = key
                                    all_attr = attr.replace(new_attr1, key.replace("#", "."))
                            max_attr_value = max_attr_value * self.table_rows[new_attr] * self.selectivity[new_attr]
                        if attr not in list(filters['Seq Scan'].keys()):
                            filters['Seq Scan'][all_attr] = frequency * plan_tree.cost
                        else:
                            filters['Seq Scan'][all_attr] += frequency * plan_tree.cost
                # 'Sort'
                if plan_tree.node_type == 'Sort':
                    for attr in plan_tree.attributes:
                        if ", " in attr:
                            attr_s = attr.split(", ")
                            max_attr_value = 0
                            for a_s in attr_s:
                                attr_value = 1
                                new_attr = a_s.replace(".", "#")
                                attr_value = attr_value * self.table_rows[new_attr] * self.selectivity[new_attr]
                                if attr_value > max_attr_value:
                                    max_attr_value = attr_value
                        else:
                            max_attr_value = 1
                            new_attr = attr.replace(".", "#")
                            max_attr_value = max_attr_value * self.table_rows[new_attr] * self.selectivity[new_attr]
                        if attr not in list(filters['Sort'].keys()):
                            filters['Sort'][attr] = frequency * plan_tree.cost
                        else:
                            filters['Sort'][attr] += frequency * plan_tree.cost
                # 'Group' && 'Aggregate': dim-3
                if plan_tree.node_type == 'Group' or plan_tree.node_type == 'Aggregate':
                    for attr in plan_tree.attributes:
                        if ", " in attr:
                            all_attr = []
                            attr_s = attr.split(", ")
                            max_attr_value = 0
                            for a_s in attr_s:
                                attr_value = 1
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == a_s:
                                        all_attr.append(a_s.replace(a_s, key.replace("#", ".")))
                                        new_attr = key
                                attr_value = attr_value * self.table_rows[new_attr] * self.selectivity[new_attr]
                                if attr_value > max_attr_value:
                                    max_attr_value = attr_value
                            all_attr = ", ".join(all_attr)
                        else:
                            max_attr_value = 1
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == attr:
                                    new_attr = key
                                    all_attr = attr.replace(attr, key.replace("#", "."))
                            max_attr_value = max_attr_value * self.table_rows[new_attr] * self.selectivity[new_attr]
                        if attr not in list(filters['Group'].keys()):
                            filters['Group'][all_attr] = frequency * plan_tree.cost
                        else:
                            filters['Group'][all_attr] += frequency * plan_tree.cost
            if len(plan_tree.children) > 0:
                filters = plan_tree.visit_children(plan_tree.children, filters, frequency)
        for key in list(filters.keys()):
            for k in list(filters[key]):
                filters[key][k] = math.log(filters[key][k] + 1e-8)
            filters[key] = dict(sorted(filters[key].items(), key=lambda item: item[1], reverse=True))
        return filters

    def workload2embedding_v2(self, workload):
        # 初始化有序字典来存储表和属性
        table_dict = OrderedDict()
        # 遍历给定的字典，按顺序添加属性
        for key in self.vocab:
            table_name, attribute = key.split('#')
            if table_name not in table_dict:
                table_dict[table_name] = []  # 使用列表来存储属性
            if attribute not in table_dict[table_name]:
                table_dict[table_name].append(attribute)
        for table_name in sorted(list(table_dict.keys()), key=len, reverse=True):
            table_dict[table_name] = sorted(table_dict[table_name], key=len, reverse=True)
        filters = dict()
        filters['Nested Loop'] = dict()
        filters['Merge Join'] = dict()
        filters['Hash Join'] = dict()
        filters['Seq Scan'] = dict()
        filters['Sort'] = dict()
        filters['Group'] = dict()
        # Plan处理
        for w in list(workload.keys()):
            plan = self.db_connector.get_plan(w)
            plan_tree = PlanTreeNode_v2().plan2tree(plan, self.vocab, table_dict)
            frequency = workload[w]
            if len(plan_tree.attributes) > 0:
                # 'Nested Loop': dim-1
                if plan_tree.node_type == 'Nested Loop':
                    for attr in plan_tree.attributes:
                        if attr not in list(filters['Nested Loop'].keys()):
                            filters['Nested Loop'][attr] = math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                        else:
                            filters['Nested Loop'][attr] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Merge Join': dim-1
                if plan_tree.node_type == 'Merge Join':
                    for attr in plan_tree.attributes:
                        if attr not in list(filters['Merge Join'].keys()):
                            filters['Merge Join'][attr] = math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                        else:
                            filters['Merge Join'][attr] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Hash Join': dim-1
                if plan_tree.node_type == 'Hash Join':
                    for attr in plan_tree.attributes:
                        if attr not in list(filters['Hash Join'].keys()):
                            filters['Hash Join'][attr] = math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                        else:
                            filters['Hash Join'][attr] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Seq Scan': dim-3
                if plan_tree.node_type == 'Seq Scan':
                    for attr in plan_tree.attributes:
                        if attr not in list(filters['Seq Scan'].keys()):
                            filters['Seq Scan'][attr] = math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                        else:
                            filters['Seq Scan'][attr] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Sort'
                if plan_tree.node_type == 'Sort':
                    for attr in plan_tree.attributes:
                        if attr not in list(filters['Sort'].keys()):
                            filters['Sort'][attr] = math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                        else:
                            filters['Sort'][attr] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Group' && 'Aggregate': dim-3
                if plan_tree.node_type == 'Group' or plan_tree.node_type == 'Aggregate':
                    for attr in plan_tree.attributes:
                        if attr not in list(filters['Group'].keys()):
                            filters['Group'][attr] = math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                        else:
                            filters['Group'][attr] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
            if len(plan_tree.children) > 0:
                filters = plan_tree.visit_children(plan_tree.children, filters, frequency)
        for key in list(filters.keys()):
            filters[key] = dict(sorted(filters[key].items(), key=lambda item: item[1], reverse=True))
        return filters

    def workload2embedding_v3(self, workload):
        # 输出列矩阵
        column_output = {}
        for column in self.vocab:
            column_output[column] = [0] * (1 * 3 + 3 * 3)
        # Plan处理
        for w in list(workload.keys()):
            plan = self.db_connector.get_plan(w)
            plan_tree = PlanTreeNode_v3().plan2tree(plan, self.vocab)
            frequency = workload[w]
            if len(plan_tree.attributes) > 0:
                # 'Nested Loop': dim-1
                if plan_tree.node_type == 'Nested Loop':
                    for attr in plan_tree.attributes:
                        column_output[attr][0] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Merge Join': dim-1
                if plan_tree.node_type == 'Merge Join':
                    for attr in plan_tree.attributes:
                        column_output[attr][1] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Hash Join': dim-1
                if plan_tree.node_type == 'Hash Join':
                    for attr in plan_tree.attributes:
                        column_output[attr][2] += math.log(frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Seq Scan': dim-3
                if plan_tree.node_type == 'Seq Scan':
                    for attr in plan_tree.attributes[0:3]:
                        column_output[attr][3 + plan_tree.attributes.index(attr)] += math.log(
                            frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Sort': dim-3
                if plan_tree.node_type == 'Sort':
                    for attr in plan_tree.attributes[0:3]:
                        column_output[attr][6 + plan_tree.attributes.index(attr)] += math.log(
                            frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
                # 'Aggregate': dim-3
                if plan_tree.node_type == 'Aggregate':
                    for attr in plan_tree.attributes[0:3]:
                        column_output[attr][9 + plan_tree.attributes.index(attr)] += math.log(
                            frequency + 1e-8) * math.log(plan_tree.cost + 1e-8)
            if len(plan_tree.children) > 0:
                column_output = plan_tree.visit_children(plan_tree.children, column_output, frequency)
        return column_output

    def process_workload(self, w):
        w['filters1'] = self.workload2embedding_v1(w['workload'])
        w['filters2'] = self.workload2embedding_v2(w['workload'])
        w['columns'] = self.workload2embedding_v3(w['workload'])
        return w


