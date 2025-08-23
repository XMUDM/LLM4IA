import re
import time
import math
import json
import copy

NOTE_TYPE = [
    'SetOp',
    'Append',
    'Unique',
    'Subquery Scan',
    'WindowAgg',
    'Gather',
    'Gather Merge',
    'Result',
    'Limit',
    'Materialize',
    'Aggregate',
    'Seq Scan',
    'Sort',
    'Nested Loop',
    'Hash',
    'Hash Join',
    'Index Scan',
    'Index Only Scan',
    'Bitmap Heap Scan',
    'Bitmap Index Scan',
    'Merge Join',
    'CTE Scan',
    'Merge Append',
    'Group',
]
USEFUL_TYPE = [
    'Nested Loop',
    'Merge Join',
    'Hash Join',
    'Seq Scan',
    'Sort',
    'Aggregate',
    'Group',
]


class PlanTreeNode_v2:
    def __init__(self, node_type=None, cost=None, plan_rows=None, plan_width=None):
        self.node_type = node_type
        self.cost = cost
        self.plan_rows = plan_rows
        self.plan_width = plan_width
        self.attributes = []
        self.height = 0
        self.children = []
        self.selectivity = json.load(open("selectivity.json"))
        self.table_rows = json.load(open("table_rows.json"))

    def plan2tree(self, plan, vocab, table_dict):
        root_node = None
        node_type = plan['Node Type']
        if node_type in NOTE_TYPE:
            cost = plan['Total Cost']
            plan_rows = plan['Plan Rows']
            plan_width = plan['Plan Width']
            root_node = PlanTreeNode_v2(node_type, cost, plan_rows, plan_width)
            root_node.cost = cost
            attributes = []
            if node_type in USEFUL_TYPE:
                if node_type == 'Nested Loop':
                    if 'Join Filter' in plan.keys():
                        # print(plan['Join Filter'])
                        new_filter = re.sub(r'_(?:[1-9])(?!\d)', '', plan['Join Filter'])
                        new_filter = new_filter.replace("(", '').replace(")", '')
                        if ' AND ' in plan['Join Filter']:
                            new_filters = new_filter.split(' AND ')
                            strs = []
                            flag_use = False
                            for filter_item in new_filters:
                                old_filter_item = filter_item
                                if ' = ' in filter_item and 'CASE WHEN' not in filter_item:
                                    items = filter_item.split(' = ')
                                    op = " = "
                                elif ' <> ' in filter_item and 'CASE WHEN' not in filter_item:
                                    items = filter_item.split(' <> ')
                                    op = " <> "
                                else:
                                    new_filters.remove(filter_item)
                                    continue
                                flag_use1 = False
                                flag_use2 = False
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[0].split('.')[1]:
                                        flag_use1 = True
                                        str1 = key.replace("#", ".")
                                if not flag_use1:
                                    filter_item = filter_item.replace(items[0], '?')
                                    str1 = " ? "
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[1].split('.')[1]:
                                        flag_use2 = True
                                        str2 = key.replace("#", ".")
                                if not flag_use2:
                                    filter_item = filter_item.replace(items[1], '?')
                                    str2 = "?"
                                strs.append(str1 + op + str2)
                                new_filters[new_filters.index(old_filter_item)] = filter_item
                                if flag_use1 or flag_use2:
                                    flag_use = True
                            if flag_use:
                                attributes.append(' AND '.join(strs))
                        else:
                            if ' = ' in new_filter:
                                items = new_filter.split(' = ')
                                op = " = "
                            else:
                                items = new_filter.split(' <> ')
                                op = " <> "
                            flag_use1 = False
                            flag_use2 = False
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[0].split('.')[1]:
                                    flag_use1 = True
                                    str1 = key.replace("#", ".")
                            if not flag_use1:
                                str1 = "?"
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[1].split('.')[1]:
                                    flag_use2 = True
                                    str2 = key.replace("#", ".")
                            if not flag_use2:
                                str2 = "?"
                            if flag_use1 or flag_use2:
                                attributes.append(str1 + op + str2)
                if node_type == 'Merge Join':
                    # print(plan['Merge Cond'])
                    new_filter = re.sub(r'_(?:[1-9])(?!\d)', '', plan['Merge Cond'])
                    new_filter = new_filter.replace("(", '').replace(")", '')
                    if ' AND ' in plan['Merge Cond']:
                        new_filters = new_filter.split(' AND ')
                        strs = []
                        flag_use = False
                        for filter_item in new_filters:
                            old_filter_item = filter_item
                            if ' = ' in filter_item:
                                items = filter_item.split(' = ')
                                op = " = "
                            else:
                                items = filter_item.split(' <> ')
                                op = " <> "
                            flag_use1 = False
                            flag_use2 = False
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[0].split('.')[1]:
                                    flag_use1 = True
                                    str1 = key.replace("#", ".")
                            if not flag_use1:
                                filter_item = filter_item.replace(items[0], '?')
                                str1 = " ? "
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[1].split('.')[1]:
                                    flag_use2 = True
                                    str2 = key.replace("#", ".")
                            if not flag_use2:
                                filter_item = filter_item.replace(items[1], '?')
                                str2 = "?"
                            strs.append(str1 + op + str2)
                            new_filters[new_filters.index(old_filter_item)] = filter_item
                            if flag_use1 or flag_use2:
                                flag_use = True
                        if flag_use:
                            attributes.append(' AND '.join(strs))
                    else:
                        if ' = ' in new_filter:
                            items = new_filter.split(' = ')
                            op = " = "
                        else:
                            items = new_filter.split(' <> ')
                            op = " <> "
                        flag_use1 = False
                        flag_use2 = False
                        for key in list(self.selectivity.keys()):
                            if key.split("#")[1] == items[0].split('.')[1]:
                                flag_use1 = True
                                str1 = key.replace("#", ".")
                        if not flag_use1:
                            str1 = "?"
                        for key in list(self.selectivity.keys()):
                            if key.split("#")[1] == items[1].split('.')[1]:
                                flag_use2 = True
                                str2 = key.replace("#", ".")
                        if not flag_use2:
                            str2 = "?"
                        if flag_use1 or flag_use2:
                            attributes.append(str1 + op + str2)
                if node_type == 'Hash Join':
                    # print(plan['Hash Cond'])
                    new_filter = re.sub(r'_(?:[1-9])(?!\d)', '', plan['Hash Cond'])
                    new_filter = new_filter.replace("(", '').replace(")", '')
                    if ' AND ' in plan['Hash Cond']:
                        new_filters = new_filter.split(' AND ')
                        strs = []
                        flag_use = False
                        for filter_item in new_filters:
                            old_filter_item = filter_item
                            if ' = ' in filter_item:
                                items = filter_item.split(' = ')
                                op = " = "
                            else:
                                items = filter_item.split(' <> ')
                                op = " <> "
                            flag_use1 = False
                            flag_use2 = False
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[0].split('.')[1]:
                                    flag_use1 = True
                                    str1 = key.replace("#", ".")
                            if not flag_use1:
                                filter_item = filter_item.replace(items[0], '?')
                                str1 = " ? "
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[1].split('.')[1]:
                                    flag_use2 = True
                                    str2 = key.replace("#", ".")
                            if not flag_use2:
                                filter_item = filter_item.replace(items[1], '?')
                                str2 = "?"
                            strs.append(str1 + op + str2)
                            new_filters[new_filters.index(old_filter_item)] = filter_item
                            if flag_use1 or flag_use2:
                                flag_use = True
                        if flag_use:
                            attributes.append(' AND '.join(strs))
                    else:
                        if ' = ' in new_filter:
                            items = new_filter.split(' = ')
                            op = " = "
                        else:
                            items = new_filter.split(' <> ')
                            op = " <> "
                        flag_use1 = False
                        flag_use2 = False
                        for key in list(self.selectivity.keys()):
                            if key.split("#")[1] == items[0].split('.')[1]:
                                flag_use1 = True
                                str1 = key.replace("#", ".")
                        if not flag_use1:
                            str1 = "?"
                        for key in list(self.selectivity.keys()):
                            if key.split("#")[1] == items[1].split('.')[1]:
                                flag_use2 = True
                                str2 = key.replace("#", ".")
                        if not flag_use2:
                            str2 = "?"
                        if flag_use1 or flag_use2:
                            attributes.append(str1 + op + str2)
                if node_type == 'Seq Scan':
                    if 'Filter' in plan.keys():
                        seq_scan_filter = plan['Filter']
                        seq_scan_filter = seq_scan_filter.replace(" OR ", ' AND ')
                        if ' AND ' in seq_scan_filter:
                            seq_scan_filters = seq_scan_filter.split(" AND ")
                            new_filters = []
                            for filter_item in seq_scan_filters:
                                if 'CASE WHEN' in filter_item:
                                    continue
                                filter_item = re.sub(r'_(?:[1-9])(?!\d)', '', filter_item)
                                filter_item = str(filter_item.replace("(", '').replace(")", ''))
                                for vb in vocab:
                                    column = vb.split("#")[1] + ' '
                                    if column in filter_item:
                                        op = filter_item.split(column)[1].split(' ')[0]
                                        new_filter_item = column + op + " ?"
                                        new_filters.append(new_filter_item)
                            if len(new_filters) > 0:
                                attributes.append(' AND '.join(new_filters[0:3]))
                        else:
                            seq_scan_filter = re.sub(r'_(?:[1-9])(?!\d)', '', seq_scan_filter)
                            seq_scan_filter = seq_scan_filter.replace("(", '').replace(")", '')
                            for vb in vocab:
                                column = vb.split("#")[1] + ' '
                                if column in seq_scan_filter:
                                    op = seq_scan_filter.split(column)[1].split(' ')[0]
                                    seq_scan_filter = column + op + " ?"
                                    attributes.append(seq_scan_filter)
                if node_type == 'Sort':
                    sort_key = list()
                    for item in plan['Sort Key']:
                        item = re.sub(r'_(?:[1-9])(?!\d)', '', item)
                        item = item.replace("(", '').replace(")", '').replace(" DESC", '').replace(" ASC", '')
                        for vb in vocab:
                            vb = vb.replace("#", ".")
                            if vb == item:
                                sort_key.append(item)
                    if len(sort_key) != 0:
                        attributes.append(', '.join(sort_key[0:3]))
                if node_type == 'Aggregate' or node_type == 'Group':
                    if 'Group Key' in plan.keys():
                        group_key = copy.deepcopy(plan['Group Key'])
                        new_group_key = []
                        for item in group_key:
                            item = re.sub(r'_(?:[1-9])(?!\d)', '', item)
                            for vb in vocab:
                                if vb.split("#")[1] in item:
                                    new_group_key.append(vb.split("#")[1])
                        if len(new_group_key) > 0:
                            attributes.append(', '.join(new_group_key[0:3]))
                root_node.attributes = attributes
            if 'Plans' in plan.keys():
                root_node.children += root_node.add_child(plan['Plans'], vocab, table_dict)
        else:
            print(node_type)
            print(plan)
            time.sleep(10000)
        return root_node

    def add_child(self, plans, vocab, table_dict):
        children = []
        for plan in plans:
            node = None
            node_type = plan['Node Type']
            if node_type in NOTE_TYPE:
                cost = plan['Total Cost']
                plan_rows = plan['Plan Rows']
                plan_width = plan['Plan Width']
                node = PlanTreeNode_v2(node_type, cost, plan_rows, plan_width)
                node.cost = cost
                attributes = []
                if node_type in USEFUL_TYPE:
                    if node_type == 'Nested Loop':
                        if 'Join Filter' in plan.keys():
                            # print(plan['Join Filter'])
                            new_filter = re.sub(r'_(?:[1-9])(?!\d)', '', plan['Join Filter'])
                            new_filter = new_filter.replace("(", '').replace(")", '')
                            if ' AND ' in plan['Join Filter']:
                                new_filters = new_filter.split(' AND ')
                                strs = []
                                flag_use = False
                                for filter_item in new_filters:
                                    old_filter_item = filter_item
                                    if ' = ' in filter_item and 'CASE WHEN' not in filter_item:
                                        items = filter_item.split(' = ')
                                        op = " = "
                                    elif ' <> ' in filter_item and 'CASE WHEN' not in filter_item:
                                        items = filter_item.split(' <> ')
                                        op = " <> "
                                    else:
                                        new_filters.remove(filter_item)
                                        continue
                                    flag_use1 = False
                                    flag_use2 = False
                                    for key in list(self.selectivity.keys()):
                                        if key.split("#")[1] == items[0].split('.')[1]:
                                            flag_use1 = True
                                            str1 = key.replace("#", ".")
                                    if not flag_use1:
                                        filter_item = filter_item.replace(items[0], '?')
                                        str1 = " ? "
                                    for key in list(self.selectivity.keys()):
                                        if key.split("#")[1] == items[1].split('.')[1]:
                                            flag_use2 = True
                                            str2 = key.replace("#", ".")
                                    if not flag_use2:
                                        filter_item = filter_item.replace(items[1], '?')
                                        str2 = "?"
                                    strs.append(str1 + op + str2)
                                    new_filters[new_filters.index(old_filter_item)] = filter_item
                                    if flag_use1 or flag_use2:
                                        flag_use = True
                                if flag_use:
                                    attributes.append(' AND '.join(strs))
                            else:
                                if ' = ' in new_filter:
                                    items = new_filter.split(' = ')
                                    op = " = "
                                else:
                                    items = new_filter.split(' <> ')
                                    op = " <> "
                                flag_use1 = False
                                flag_use2 = False
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[0].split('.')[1]:
                                        flag_use1 = True
                                        str1 = key.replace("#", ".")
                                if not flag_use1:
                                    str1 = "?"
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[1].split('.')[1]:
                                        flag_use2 = True
                                        str2 = key.replace("#", ".")
                                if not flag_use2:
                                    str2 = "?"
                                if flag_use1 or flag_use2:
                                    attributes.append(str1 + op + str2)
                    if node_type == 'Merge Join':
                        # print(plan['Merge Cond'])
                        new_filter = re.sub(r'_(?:[1-9])(?!\d)', '', plan['Merge Cond'])
                        new_filter = new_filter.replace("(", '').replace(")", '')
                        if ' AND ' in plan['Merge Cond']:
                            new_filters = new_filter.split(' AND ')
                            strs = []
                            flag_use = False
                            for filter_item in new_filters:
                                old_filter_item = filter_item
                                if ' = ' in filter_item:
                                    items = filter_item.split(' = ')
                                    op = " = "
                                else:
                                    items = filter_item.split(' <> ')
                                    op = " <> "
                                flag_use1 = False
                                flag_use2 = False
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[0].split('.')[1]:
                                        flag_use1 = True
                                        str1 = key.replace("#", ".")
                                if not flag_use1:
                                    filter_item = filter_item.replace(items[0], '?')
                                    str1 = " ? "
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[1].split('.')[1]:
                                        flag_use2 = True
                                        str2 = key.replace("#", ".")
                                if not flag_use2:
                                    filter_item = filter_item.replace(items[1], '?')
                                    str2 = "?"
                                strs.append(str1 + op + str2)
                                new_filters[new_filters.index(old_filter_item)] = filter_item
                                if flag_use1 or flag_use2:
                                    flag_use = True
                            if flag_use:
                                attributes.append(' AND '.join(strs))
                        else:
                            if ' = ' in new_filter:
                                items = new_filter.split(' = ')
                                op = " = "
                            else:
                                items = new_filter.split(' <> ')
                                op = " <> "
                            flag_use1 = False
                            flag_use2 = False
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[0].split('.')[1]:
                                    flag_use1 = True
                                    str1 = key.replace("#", ".")
                            if not flag_use1:
                                str1 = "?"
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[1].split('.')[1]:
                                    flag_use2 = True
                                    str2 = key.replace("#", ".")
                            if not flag_use2:
                                str2 = "?"
                            if flag_use1 or flag_use2:
                                attributes.append(str1 + op + str2)
                    if node_type == 'Hash Join':
                        # print(plan['Hash Cond'])
                        new_filter = re.sub(r'_(?:[1-9])(?!\d)', '', plan['Hash Cond'])
                        new_filter = new_filter.replace("(", '').replace(")", '')
                        if ' AND ' in plan['Hash Cond']:
                            new_filters = new_filter.split(' AND ')
                            strs = []
                            flag_use = False
                            for filter_item in new_filters:
                                old_filter_item = filter_item
                                if ' = ' in filter_item:
                                    items = filter_item.split(' = ')
                                    op = " = "
                                else:
                                    items = filter_item.split(' <> ')
                                    op = " <> "
                                flag_use1 = False
                                flag_use2 = False
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[0].split('.')[1]:
                                        flag_use1 = True
                                        str1 = key.replace("#", ".")
                                if not flag_use1:
                                    filter_item = filter_item.replace(items[0], '?')
                                    str1 = " ? "
                                for key in list(self.selectivity.keys()):
                                    if key.split("#")[1] == items[1].split('.')[1]:
                                        flag_use2 = True
                                        str2 = key.replace("#", ".")
                                if not flag_use2:
                                    filter_item = filter_item.replace(items[1], '?')
                                    str2 = "?"
                                strs.append(str1 + op + str2)
                                new_filters[new_filters.index(old_filter_item)] = filter_item
                                if flag_use1 or flag_use2:
                                    flag_use = True
                            if flag_use:
                                attributes.append(' AND '.join(strs))
                        else:
                            if ' = ' in new_filter:
                                items = new_filter.split(' = ')
                                op = " = "
                            else:
                                items = new_filter.split(' <> ')
                                op = " <> "
                            flag_use1 = False
                            flag_use2 = False
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[0].split('.')[1]:
                                    flag_use1 = True
                                    str1 = key.replace("#", ".")
                            if not flag_use1:
                                str1 = "?"
                            for key in list(self.selectivity.keys()):
                                if key.split("#")[1] == items[1].split('.')[1]:
                                    flag_use2 = True
                                    str2 = key.replace("#", ".")
                            if not flag_use2:
                                str2 = "?"
                            if flag_use1 or flag_use2:
                                attributes.append(str1 + op + str2)
                    if node_type == 'Seq Scan':
                        if 'Filter' in plan.keys():
                            seq_scan_filter = plan['Filter']
                            seq_scan_filter = seq_scan_filter.replace(" OR ", ' AND ')
                            if ' AND ' in seq_scan_filter:
                                seq_scan_filters = seq_scan_filter.split(" AND ")
                                new_filters = []
                                for filter_item in seq_scan_filters:
                                    if 'CASE WHEN' in filter_item:
                                        continue
                                    filter_item = re.sub(r'_(?:[1-9])(?!\d)', '', filter_item)
                                    filter_item = str(filter_item.replace("(", '').replace(")", ''))
                                    for vb in vocab:
                                        column = vb.split("#")[1] + ' '
                                        if column in filter_item:
                                            op = filter_item.split(column)[1].split(' ')[0]
                                            new_filter_item = column + op + " ?"
                                            new_filters.append(new_filter_item)
                                if len(new_filters) > 0:
                                    attributes.append(' AND '.join(new_filters[0:3]))
                            else:
                                seq_scan_filter = re.sub(r'_(?:[1-9])(?!\d)', '', seq_scan_filter)
                                seq_scan_filter = seq_scan_filter.replace("(", '').replace(")", '')
                                for vb in vocab:
                                    column = vb.split("#")[1] + ' '
                                    if column in seq_scan_filter:
                                        op = seq_scan_filter.split(column)[1].split(' ')[0]
                                        seq_scan_filter = column + op + " ?"
                                        attributes.append(seq_scan_filter)
                    if node_type == 'Sort':
                        sort_key = list()
                        for item in plan['Sort Key']:
                            item = re.sub(r'_(?:[1-9])(?!\d)', '', item)
                            item = item.replace("(", '').replace(")", '').replace(" DESC", '').replace(" ASC", '')
                            for vb in vocab:
                                vb = vb.replace("#", ".")
                                if vb == item:
                                    sort_key.append(item)
                        if len(sort_key) != 0:
                            attributes.append(', '.join(sort_key[0:3]))
                    if node_type == 'Aggregate' or node_type == 'Group':
                        if 'Group Key' in plan.keys():
                            if 'Group Key' in plan.keys():
                                group_key = copy.deepcopy(plan['Group Key'])
                                new_group_key = []
                                for item in group_key:
                                    item = re.sub(r'_(?:[1-9])(?!\d)', '', item)
                                    for vb in vocab:
                                        if vb.split("#")[1] in item:
                                            new_group_key.append(vb.split("#")[1])
                                if len(new_group_key) > 0:
                                    attributes.append(', '.join(new_group_key[0:3]))
                    node.attributes = attributes
                if 'Plans' in plan.keys():
                    node.children += node.add_child(plan['Plans'], vocab, table_dict)
            else:
                print(plan)
                time.sleep(10000)
            children.append(node)
        return children

    def visit_children(self, nodes, filters, frequency):
        for node in nodes:
            if len(node.attributes) > 0:
                # 'Nested Loop': dim-1
                if node.node_type == 'Nested Loop':
                    for attr in node.attributes:
                        if attr not in list(filters['Nested Loop'].keys()):
                            filters['Nested Loop'][attr] = math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                        else:
                            filters['Nested Loop'][attr] += math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                # 'Merge Join': dim-1
                if node.node_type == 'Merge Join':
                    for attr in node.attributes:
                        if attr not in list(filters['Merge Join'].keys()):
                            filters['Merge Join'][attr] = math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                        else:
                            filters['Merge Join'][attr] += math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                # 'Hash Join': dim-1
                if node.node_type == 'Hash Join':
                    for attr in node.attributes:
                        if attr not in list(filters['Hash Join'].keys()):
                            filters['Hash Join'][attr] = math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                        else:
                            filters['Hash Join'][attr] += math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                # 'Seq Scan': dim-3
                if node.node_type == 'Seq Scan':
                    for attr in node.attributes:
                        if attr not in list(filters['Seq Scan'].keys()):
                            filters['Seq Scan'][attr] = math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                        else:
                            filters['Seq Scan'][attr] += math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                # 'Sort'
                if node.node_type == 'Sort':
                    for attr in node.attributes:
                        if attr not in list(filters['Sort'].keys()):
                            filters['Sort'][attr] = math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                        else:
                            filters['Sort'][attr] += math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                # 'Group' && 'Aggregate': dim-3
                if node.node_type == 'Group' or node.node_type == 'Aggregate':
                    for attr in node.attributes:
                        if attr not in list(filters['Group'].keys()):
                            filters['Group'][attr] = math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
                        else:
                            filters['Group'][attr] += math.log(frequency + 1e-8) * math.log(node.cost + 1e-8)
            if len(node.children) > 0:
                filters = node.visit_children(node.children, filters, frequency)
        return filters