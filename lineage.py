#!/usr/bin/env python
# coding:utf-8
from json import dumps
from flask import Flask, g, Response, request
from neo4j.v1 import GraphDatabase, basic_auth
debugFlag = True

app = Flask(__name__)
driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth("neo4j", "root"))

"""
db_run_switch[dep][flag]
dep = 0(依赖) | 1(被依赖)
flag = 0(table) | 1(column)
"""
db_run_switch = {
    "0": {
        "0": "MATCH (n:Table)-[r:depTable*]->(m:Table) WHERE n.name = {name} "
             "RETURN n as source,m as target,r as relation_list",
        "1": "MATCH (n:Column)-[r:depColumn*]->(m:Column) WHERE n.name = {name} "
             "RETURN n as source,m as target,r as relation_list"
    },
    "1": {
        "0": "MATCH (n:Table)-[r:depTable*]->(m:Table) WHERE m.name = {name} "
             "RETURN m as source,n as target,r as relation_list",
        "1": "MATCH (n:Column)-[r:depColumn*]->(m:Column) WHERE m.name = {name} "
             "RETURN m as source,n as target,r as relation_list"
    }
}


def get_db():
    if not hasattr(g, 'lineage_analysis.db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'lineage_analysis.db'):
        g.neo4j_db.close()


def chang_name(table_name_and_id_dict, children_list):
    for elem in children_list:
        elem['name'] = table_name_and_id_dict[elem['name']]
        if 'children' in elem:
            elem['children'] = chang_name(table_name_and_id_dict, elem['children'])
    return children_list


@app.route("/lineage")
def get_graph_table():
    try:
        name = request.args["name"]
        dep = request.args["dep"]
        flag = request.args["flag"]
    except KeyError:
        return "KeyError"
    else:
        return Response(build_tree(name, dep, flag), mimetype="application/json")


def process_dep_relation(results, table_name_and_id_dict, tree_json_dict):
    for recode in results:
        target_id = str(recode['target'].id)
        target_name = recode['target']['name']
        table_name_and_id_dict.setdefault(target_id, target_name)
        # 下面是主题
        tree_json_dict.setdefault('children', [])
        # tmp是一个list
        tmp = tree_json_dict['children']
        for deep_relation in recode['relation_list']:
            deep_relation_end = deep_relation.end
            if len(tmp) != 0 and str(deep_relation_end) == tmp[len(tmp) - 1]["name"]:
                tmp[len(tmp) - 1].setdefault('children', [])
                tmp = tmp[len(tmp) - 1]['children']
            else:
                new_node = dict()
                new_node['name'] = str(deep_relation_end)
                tmp.append(new_node)
                tmp[len(tmp) - 1]["size"] = int(tmp[len(tmp) - 1]["name"])
    return table_name_and_id_dict, tree_json_dict


def process_be_dep_relation(results, table_name_and_id_dict, tree_json_dict):
    for recode in results:
        target_id = str(recode['target'].id)
        target_name = recode['target']['name']
        table_name_and_id_dict.setdefault(target_id, target_name)
        # 下面是主题
        tree_json_dict.setdefault('children', [])
        # tmp是一个list
        tmp = tree_json_dict['children']
        for deep_relation in reversed(recode['relation_list']):
            deep_relation_end = deep_relation.start
            if len(tmp) != 0 and str(deep_relation_end) == tmp[len(tmp) - 1]["name"]:
                tmp[len(tmp) - 1].setdefault('children', [])
                tmp = tmp[len(tmp) - 1]['children']
            else:
                new_node = dict()
                new_node['name'] = str(deep_relation_end)
                tmp.append(new_node)
                tmp[len(tmp) - 1]["size"] = int(tmp[len(tmp) - 1]["name"])
    return table_name_and_id_dict, tree_json_dict


def build_tree(name, dep, flag):
    db = get_db()
    results = db.run(db_run_switch[dep][flag], {"name": name})
    # 存放name和id对应的关系
    table_name_and_id_dict = dict()
    table_name_and_id_dict['0'] = name
    # 最后输出结果
    tree_json_dict = dict()
    tree_json_dict['name'] = '0'

    if dep == "0":
        table_name_and_id_dict, tree_json_dict = process_dep_relation(
            results, table_name_and_id_dict,tree_json_dict)
    else:
        table_name_and_id_dict, tree_json_dict = process_be_dep_relation(
            results, table_name_and_id_dict,tree_json_dict)

    tree_json_dict['name'] = table_name_and_id_dict[tree_json_dict['name']]
    print table_name_and_id_dict
    print tree_json_dict
    if 'children' in tree_json_dict:
        tree_json_dict['children'] = chang_name(table_name_and_id_dict, tree_json_dict['children'])
    return dumps(tree_json_dict)


if __name__ == '__main__':
    app.run(port=8080)
