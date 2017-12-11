import time
import json
import sys
import mysql.connector
from datetime import datetime

def init_post(esindex,es,syncid_post,dbuser,dbhost,dbpwd,dbname):
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
      repo longtext,\
      fork longtext,\
      post_type longtext,\
      author_id bigint(20),\
      author_username longtext,\
      star_num bigint(20),\
      reply_num bigint(20),\
      elite tinyint(1),\
      created datetime,\
      PRIMARY KEY (id)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("DROP TRIGGER IF EXISTS synces_post_insert")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TRIGGER synces_post_insert \
        AFTER INSERT ON a2post \
        FOR EACH ROW \
        INSERT INTO synces_post (recordid,timestamp, opcode, title, description,cover,name,repo,fork,post_type,author_id,author_username,star_num,reply_num,elite,created) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 1, NEW.title, NEW.description, NEW.cover, NEW.name, NEW.repo,NEW.fork,NEW.post_type,NEW.author_id,NEW.author_username,NEW.star_num,NEW.reply_num,NEW.elite,NEW.created) \
    ")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("DROP TRIGGER IF EXISTS synces_post_update")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TRIGGER synces_post_update \
        AFTER UPDATE ON a2post \
        FOR EACH ROW \
        INSERT INTO synces_post (recordid,timestamp, opcode, title, description,cover,name,repo,fork,post_type,author_id,author_username,star_num,reply_num,elite,created) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 2, NEW.title, NEW.description, NEW.cover, NEW.name, NEW.repo,NEW.fork,NEW.post_type,NEW.author_id,NEW.author_username,NEW.star_num,NEW.reply_num,NEW.elite,NEW.created) \
    ")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("DROP TRIGGER IF EXISTS synces_post_delete")
    cursor.execute(managePostTriggers)

    managePostTriggers = ("CREATE TRIGGER synces_post_delete \
        BEFORE DELETE ON a2post \
        FOR EACH ROW \
        INSERT INTO synces_post (recordid,timestamp, opcode, title, description,cover,name,repo,fork,post_type,author_id,author_username,star_num,reply_num,elite,created) \
        VALUES (OLD.id, UNIX_TIMESTAMP(), 3, OLD.title, OLD.description, OLD.cover, OLD.name,OLD.repo,OLD.fork,OLD.post_type,OLD.author_id,OLD.author_username,OLD.star_num,OLD.reply_num,OLD.elite,OLD.created) \
    ")
    cursor.execute(managePostTriggers)

    queryRecords = ("SELECT * FROM a2post")
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
                    'id':recordItem[0],
                    'title':recordItem[1],
                    'description':recordItem[2],
                    'cover':recordItem[3],
                    'name':recordItem[4],
                    'repo':recordItem[5],
                    'fork':recordItem[6],
                    'post_type':recordItem[7],
                    'author_id':recordItem[8],
                    'author_username':recordItem[9],
                    'star_num':recordItem[12],
                    'reply_num':recordItem[13],
                    'elite':recordItem[14],
                    'created':recordItem[15]
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[0], body=doc)
                print('add/update post record %s => %s' %(recordItem[0],res))

    cnx.commit()
    cursor.close()
    cnx.close()

    return syncid_post

def job_post(syncid_post,dbuser,dbhost,dbpwd,dbname,es,esindex):
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
                    'id':recordItem[1],
                    'title':recordItem[4],
                    'description':recordItem[5],
                    'cover':recordItem[6],
                    'name':recordItem[7],
                    'repo':recordItem[8],
                    'fork':recordItem[9],
                    'post_type':recordItem[10],
                    'author_id':recordItem[11],
                    'author_username':recordItem[12],
                    'star_num':recordItem[13],
                    'reply_num':recordItem[14],
                    'elite':recordItem[15],
                    'created':recordItem[16]
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

    return syncid_post

