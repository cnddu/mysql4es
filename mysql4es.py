import time
import json
import sys
import mysql.connector
from datetime import datetime
import schedule
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

esindex = "cnddu"

syncid_user = 0
syncid_post = 0

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

def init_user(esindex):
    global es,syncid_user,dbuser,dbhost,dbpwd,dbname
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname)
    cursor = cnx.cursor()

    #init for user table
    manageUserTriggers = ("DROP TABLE IF EXISTS synces_user")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TABLE synces_user (\
      id int(11) NOT NULL AUTO_INCREMENT,\
      recordid int(11),\
      timestamp int unsigned,\
      opcode tinyint,\
      username text,\
      nickname text,\
      avatar text,\
      description text,\
      PRIMARY KEY (id)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("DROP TRIGGER IF EXISTS synces_user_insert")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TRIGGER synces_user_insert \
        AFTER INSERT ON user \
        FOR EACH ROW \
        INSERT INTO synces_user (recordid,timestamp, opcode, username, nickname, avatar, description) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 1, NEW.username, NEW.nickname, NEW.avatar, NEW.description) \
    ")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("DROP TRIGGER IF EXISTS synces_user_update")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TRIGGER synces_user_update \
        AFTER UPDATE ON user \
        FOR EACH ROW \
        INSERT INTO synces_user (recordid,timestamp, opcode, username, nickname, avatar, description) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 2, NEW.username, NEW.nickname, NEW.avatar, NEW.description) \
    ")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("DROP TRIGGER IF EXISTS synces_user_delete")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TRIGGER synces_user_delete \
        BEFORE DELETE ON user \
        FOR EACH ROW \
        INSERT INTO synces_user (recordid,timestamp, opcode, username, nickname, avatar, description) \
        VALUES (OLD.id, UNIX_TIMESTAMP(), 3, OLD.username, OLD.nickname, OLD.avatar, OLD.description) \
    ")
    cursor.execute(manageUserTriggers)

    queryRecords = ("SELECT * FROM user")
    cursor.execute(queryRecords)
    records = cursor.fetchall()

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
                'userid':recordItem[0],
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

def init_post(esindex):
    global es,syncid_post,dbuser,dbhost,dbpwd,dbname
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname)
    cursor = cnx.cursor()

    #init for post table
    managePostTriggers = ("DROP TABLE IF EXISTS synces_post")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TABLE synces_post (\
      id int(11) NOT NULL AUTO_INCREMENT,\
      recordid int(11),\
      timestamp int unsigned,\
      opcode tinyint,\
      title longtext,\
      description longtext,\
      cover longtext,\
      name longtext,\
      PRIMARY KEY (id)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("DROP TRIGGER IF EXISTS synces_post_insert")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TRIGGER synces_post_insert \
        AFTER INSERT ON post \
        FOR EACH ROW \
        INSERT INTO synces_post (recordid,timestamp, opcode, title, description,cover,name) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 1, NEW.title, NEW.description, NEW.cover, NEW.name) \
    ")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("DROP TRIGGER IF EXISTS synces_post_update")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TRIGGER synces_post_update \
        AFTER UPDATE ON post \
        FOR EACH ROW \
        INSERT INTO synces_post (recordid,timestamp, opcode, title, description,cover,name) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 2, NEW.title, NEW.description, NEW.cover, NEW.name) \
    ")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("DROP TRIGGER IF EXISTS synces_post_delete")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TRIGGER synces_post_delete \
        BEFORE DELETE ON post \
        FOR EACH ROW \
        INSERT INTO synces_post (recordid,timestamp, opcode, title, description,cover,name) \
        VALUES (OLD.id, UNIX_TIMESTAMP(), 3, OLD.title, OLD.description, OLD.cover, OLD.name) \
    ")
    cursor.execute(managePostTriggers)

    queryRecords = ("SELECT * FROM post")
    cursor.execute(queryRecords)
    records = cursor.fetchall()

    estype = "post"
    res = es.indices.exists_type(index=esindex,doc_type=estype)
    print('check type exists: %s => %s' %(estype,res))
    if (res == False):
        mappingstr = {
            estype: {
                "_all": {
                    "enabled": "false"
                },
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word",
                        "include_in_all": "true",
                        "boost": 8
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word",
                        "include_in_all": "true",
                        "boost": 8
                    },
                    "cover": {
                        "type": "text",
                        "include_in_all": "true"
                    },
                    "name": {
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
                doc = {
                    'postid':recordItem[0],
                    'title':recordItem[1],
                    'description':recordItem[2],
                    'cover':recordItem[3],
                    'name':recordItem[4],
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[0], body=doc)
                print('add/update post record %s => %s' %(recordItem[0],res))

    cnx.commit()
    cursor.close()
    cnx.close()

def job_user():
    global syncid_user,dbuser,dbhost,dbpwd,dbname,es
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname, connection_timeout=100)
    cursor = cnx.cursor()

    estype = "user"
    print("working on sync table user => use syncid %d" %syncid_user)

    querySync = ("SELECT * FROM synces_user WHERE id>"+str(syncid_user)+" ORDER BY id ASC")
    cursor.execute(querySync)
    records = cursor.fetchall()
    print (records)

    if (len(records) > 0):
        for recordItem in records:
            print(recordItem)
            if (recordItem[3] == 1) or (recordItem[3] == 2): #insert
                doc = {
                    'userid':recordItem[1],
                    'username':recordItem[4],
                    'nickname':recordItem[5],
                    'avatar':recordItem[6],
                    'description':recordItem[7],
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[1], body=doc)
                print('add/update user record %s => %s' %(recordItem[1],res))
            elif (recordItem[3] == 3): #delete
                try:
                    res = es.delete(index=esindex, doc_type=estype, id=recordItem[1])
                    print('delete doc %s => %s' %(recordItem[1],res))
                except NotFoundError, e:
                    print('doc %s found? %s' %(recordItem[1],e.info['found']))
        syncid_user = recordItem[0]
        print ("update sync id: %d" %syncid_user)

    cnx.commit()
    cursor.close()
    cnx.close()

def job_post():
    global syncid_post,dbuser,dbhost,dbpwd,dbname,es
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname, connection_timeout=100)
    cursor = cnx.cursor()

    estype = "post"
    print("working on sync table post => use syncid %d" %syncid_post)

    querySync = ("SELECT * FROM synces_post WHERE id>"+str(syncid_post)+" ORDER BY id ASC")
    cursor.execute(querySync)
    records = cursor.fetchall()
    print (records)

    if (len(records) > 0):
        for recordItem in records:
            print(recordItem)
            if (recordItem[3] == 1) or (recordItem[3] == 2): #insert
                doc = {
                    'postid':recordItem[1],
                    'title':recordItem[4],
                    'description':recordItem[5],
                    'cover':recordItem[6],
                    'name':recordItem[7]
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[1], body=doc)
                print('add/update post record %s => %s' %(recordItem[1],res))
            elif (recordItem[3] == 3): #delete
                try:
                    res = es.delete(index=esindex, doc_type=estype, id=recordItem[1])
                    print('delete doc %s => %s' %(recordItem[1],res))
                except NotFoundError, e:
                    print('doc %s found? %s' %(recordItem[1],e.info['found']))
        syncid_post = recordItem[0]
        print ("update sync id: %d" %syncid_post)

    cnx.commit()
    cursor.close()
    cnx.close()

def job():
    print('start job ====> user')
    job_user()
    print('end job <==== user')
    print('start job ====> post')
    job_post()
    print('end job <==== post')

#main
reset_esindex(esindex)
init_user(esindex)
init_post(esindex)

schedule.every(10).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
