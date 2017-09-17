import time
import json
import sys
import mysql.connector
from datetime import datetime
import schedule
from elasticsearch import Elasticsearch,NotFoundError, RequestsHttpConnection, serializer, compat, exceptions

from table_user import init_user,job_user
from table_post import init_post,job_post
from table_booklist import init_booklist,job_booklist
from table_tag import init_tag,job_tag

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

esindex = "cnddu"

syncid_user = 0
syncid_post = 0
syncid_booklist = 0
syncid_tag = 0

dbhost = 'rm-bp18giw6ai77y6171o.mysql.rds.aliyuncs.com'
dbuser = 'root'
dbpwd = 'Glj@2017'
dbname = 'love2io'

def reset_esindex(esindex):
    global es 
    #delete and recreate index
    res = es.indices.exists(index=esindex)
    print('check index exists: %s => %s' %(esindex,res))
    if (res == True):
        res = es.indices.delete(index=esindex)
        print('delete index: %s => %s' %(esindex,res))
    res = es.indices.create(index=esindex)
    print('create index: %s => %s' %(esindex,res))


def job():
    global syncid_user,syncid_post,dbuser,dbhost,dbpwd,dbname,es
    print('start job ====> user')
    syncid_user = job_user(syncid_user,dbuser,dbhost,dbpwd,dbname,es)
    print('end job <==== user')
    print('start job ====> post')
    syncid_post = job_post(syncid_post,dbuser,dbhost,dbpwd,dbname,es)
    print('end job <==== post')

#main
reset_esindex(esindex)
syncid_user = init_user(esindex,es,syncid_user,dbuser,dbhost,dbpwd,dbname)
syncid_post = init_post(esindex,es,syncid_post,dbuser,dbhost,dbpwd,dbname)
syncid_booklist = init_booklist(esindex,es,syncid_booklist,dbuser,dbhost,dbpwd,dbname)
syncid_tag = init_tag(esindex,es,syncid_tag,dbuser,dbhost,dbpwd,dbname)

schedule.every(10).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
