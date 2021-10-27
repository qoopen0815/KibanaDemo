# -*- coding: utf-8 -*-
"""
Created on Thu Jun 17 21:10:55 2021

@author: ku_sk
"""
from elasticsearch import Elasticsearch
from csv import DictReader
import json

csv_file = "sample_data\sample.csv"
elasticsearch_host = "localhost"
index_name = "sample-data"
doctype = "Sample"

es = Elasticsearch([f"http://{elasticsearch_host}:9200"])

mapping = json.load(open('configs/mapping.json', 'r'))

# いまだけ
es.indices.delete(index=index_name)

# インデックスが存在しなければ新規作成
if not es.indices.exists(index=index_name):
  es.indices.create(index=index_name)

# mapの更新（確認？）
# include_type_nameはmappingにtypeを含む場合にTrueにする（無ければ書かなくていい
es.indices.put_mapping(index=index_name, doc_type=doctype, body=mapping, include_type_name=True)

# print(es.indices.get_mapping(index=index_name))

### simple
with open(csv_file) as file:
  for row in DictReader(file):
    es.index(index=index_name, doc_type=doctype, id=row["Date/Time"], body=row)

file.close()
