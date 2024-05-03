import mysql.connector
import requests
import json

# MySQL连接配置
mysql_config = {
    'host': 'xx.xx.xxx.xx',
    'port': 'xx',
    'user': 'xx',
    'password': 'xxxxxx',
    'database': 'xxxxxxx'
}

# Elasticsearch配置
es_host = '127.0.0.1'
es_port = '9200'
es_index = 'order1'


# 新版本es不需求type字段
# es_type = '_doc'

def fetch_mysql_table_fields(mysql_config):
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()

    # 获取MySQL表字段信息,指定需要转换得表名
    cursor.execute(f"DESCRIBE {'`housesalegj`'}")
    fields = cursor.fetchall()
    cursor.close()
    connection.close()

    return fields


def generate_es_mapping(fields):
    mapping = {
        "mappings": {
            "properties": {}
        }
    }

    for field in fields:
        field_name = field[0]
        field_type = field[1]

        # 根据MySQL字段类型设置Elasticsearch映射类型
        es_field_type = "text"  # 默认为文本类型
        if "int" in field_type:
            es_field_type = "integer"
        elif "bigint" in field_type:
            es_field_type = "long"
        elif "tinyint" in field_type:
            es_field_type = "short"
        elif "float" in field_type:
            es_field_type = "float"
        elif "double" in field_type:
            es_field_type = "double"
        elif "decimal" in field_type:
            es_field_type = "double"
        elif "date" in field_type or "datetime" in field_type or "timestamp" in field_type or "time" in field_type:
            es_field_type = "date"
        elif "json" in field_type:
            es_field_type = "object"

        # 这里可以根据需要添加更多类型的映射

        mapping["mappings"]["properties"][field_name] = {
            "type": es_field_type
        }

    return mapping


def print_es_mapping(mapping):
    print(json.dumps(mapping, indent=2))


def create_es_index_mapping(es_host, es_port, es_index, mapping):
    url = f"http://{es_host}:{es_port}/{es_index}"
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps(mapping)

    response = requests.put(url, headers=headers, data=payload)

    if response.status_code == 200:
        print(f"Elasticsearch index mapping created for index '{es_index}'")
    else:
        print(f"Failed to create Elasticsearch index mapping. Status code: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    # 获取MySQL表字段信息
    table_fields = fetch_mysql_table_fields(mysql_config)

    # 生成Elasticsearch Mapping
    es_mapping = generate_es_mapping(table_fields)

    # 打印Elasticsearch Mapping到控制台
    print_es_mapping(es_mapping)
