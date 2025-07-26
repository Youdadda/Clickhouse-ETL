#### WinlogBeat table
from infi.clickhouse_orm import Database
from tableschemas import Winlogbeat
from config import pipeline_logger
import requests
import json
import re
from clickhouse_driver import Client
from datetime import datetime
from Opensearch_basic_function import *
from Clickhouse_basic_functions import *
from opensearchpy import OpenSearch
import os
from dotenv import load_dotenv
load_dotenv()


client = OpenSearch(
    hosts=[{'host': os.environ.get("OPENSEARCH_HOST"),
             'port': os.environ.get("OPENSEARCH_PORT")}]
)

 
creation = Create_clickhouse_table(
    database_name=os.environ.get("ClickHouse_DATABASE"),
    db_url=f'http://{os.environ.get("ClickHouse_HOST")}:{os.environ.get("ClickHouse_PORT_ORM")}/',
    table=Winlogbeat,
    password=os.environ.get("ClickHouse_PASSWORD"),
    user_name=os.environ.get("ClickHouse_USER")

)

index = os.environ.get("OPENSEARCH_INDEX")
batch_size = 2
sort_fields = ['@timestamp', '_id']
search_after = None

clickhouse_client = Client(
    host=os.environ.get("ClickHouse_HOST"),
    port=os.environ.get("ClickHouse_PORT_DRIVER"),         
    user=os.environ.get("ClickHouse_USER"),
    password=os.environ.get("ClickHouse_PASSWORD"),
    database=os.environ.get("ClickHouse_DATABASE")
)
while True:
    body = {
        "size": batch_size,
        "sort": [{ "@timestamp": "asc" }, { "_id": "asc" }],
        "query": {
            "match_all": {}
        }
    }

    if search_after:
        body["search_after"] = search_after

    response = client.search(index=index, body=body)

    hits = response['hits']['hits']
    if not hits:
        break
    rows = []
    # Process your batch
    for doc in hits:
        rows.append(extract_columns(doc['_source']))

    # Prepare search_after for the next page
    search_after = hits[-1]['sort']
    
    try:
        write_data_to_clickhouse(
            host=os.environ.get("ClickHouse_HOST"),
            port=os.environ.get("ClickHouse_PORT_DRIVER"),
            password=os.environ.get("ClickHouse_PASSWORD"),
            database=os.environ.get("ClickHouse_DATABASE"),
            rows=rows,
            user=os.environ.get("ClickHouse_USER"),
            table=Winlogbeat
            
        ) 
        pipeline_logger.info("rows pushed successfully!")
            
    except Exception as e:
        pipeline_logger.error(f"We had a poblem pushing the row because : {e}")
 