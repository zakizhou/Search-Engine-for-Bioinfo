from elasticsearch import Elasticsearch
from json import load
import codecs
import os


jsons = os.listdir("bio/json")
es = Elasticsearch()
for i, f_name in enumerate(jsons):
    index = int(f_name.split(".")[0])
    path = os.path.join("bio/json", f_name)
    buffer = codecs.open(path)
    json = load(buffer)
    json['index'] = index
    print("insert record %d to elastic" % (i+1))
    try:
        es.index(index="omim", doc_type="disease", id=index, body=json)
    except KeyboardInterrupt:
        "insertion failed, deleting all records!"
        es.indices.delete(index="omim", ingore=[400, 404])
