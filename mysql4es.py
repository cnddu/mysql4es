import time
import json
import sys
import mysql.connector
from datetime import datetime
from elasticsearch import Elasticsearch,NotFoundError, RequestsHttpConnection, serializer, compat, exceptions

class JSONSerializerPython2(serializer.JSONSerializer):
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

es = Elasticsearch(serializer=JSONSerializerPython2())


cnx = mysql.connector.connect(user='root', host='121.42.235.121', password='lovepan_0808', database='love2io')
cursor = cnx.cursor()

queryRecords = ("SELECT * FROM user")
cursor.execute(queryRecords)
records = cursor.fetchall()

esindex = "cnddu"
res = es.indices.exists(index=esindex)
print('check index exists: %s => %s' %(esindex,res))
if (res == False):
    res = es.indices.create(index=esindex)
    print('create index: %s => %s' %(esindex,res))

estype = "user"
res = es.indices.exists_type(index=esindex,doc_type=estype)
print('check type exists: %s => %s' %(estype,res))
if (res == False):
    mappingstr = {
        estype: {
            "_all": {
                "enabled": "false"
            },
            "properties": {
                "username": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_max_word",
                    "include_in_all": "true",
                    "boost": 8
                },
                "nickname": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_max_word",
                    "include_in_all": "true",
                    "boost": 8
                },
                "avatar": {
                    "type": "text",
                    "include_in_all": "true"
                },
                "description": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_max_word",
                    "include_in_all": "true",
                    "boost": 8
                }
            }
        }
    }
    res = es.indices.put_mapping(index=esindex,doc_type=estype,body=mappingstr)
    print('create mapping %s => %s' %(estype,res))

if (records is not None):
    for recordItem in records:
        #fpList.append(recordItem)
        print(recordItem)
        try:
            docexist = True
            res = es.get(index=esindex, doc_type=estype, id=recordItem[0])
            print('search for user record id: %s => %s' %(recordItem[0],res))
        except NotFoundError, e:
            docexist = e.info['found']
        #time.sleep(10000);
        doc = {
                'username':recordItem[1],
                'nickname':recordItem[7],
                'avatar':recordItem[2],
                'description':recordItem[10],
        }
        res = es.index(index=esindex, doc_type=estype, id=recordItem[0], body=doc)
        print('add/update user record %s => %s' %(recordItem[0],res))


cnx.commit()
cursor.close()
cnx.close()

