import time
import json
import sys
import mysql.connector
from datetime import datetime

def init_booklist(esindex,es,syncid_booklist,dbuser,dbhost,dbpwd,dbname):
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname)
    cursor = cnx.cursor()

    #init for booklist table
    manageBooklistTriggers = ("DROP TABLE IF EXISTS synces_booklist")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("CREATE TABLE synces_booklist (\
      id int(11) NOT NULL AUTO_INCREMENT,\
      recordid int(11),\
      timestamp int unsigned,\
      opcode tinyint,\
      title longtext,\
      description longtext,\
      uuid longtext,\
      cover longtext,\
      author_id bigint(20),\
      post_num bigint(20),\
      follow_num bigint(20),\
      reply_num bigint(20),\
      elite tinyint(1),\
      updated datetime,\
      created datetime,\
      PRIMARY KEY (id)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("DROP TRIGGER IF EXISTS synces_booklist_insert")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("CREATE TRIGGER synces_booklist_insert \
        AFTER INSERT ON a2booklist \
        FOR EACH ROW \
        INSERT INTO synces_booklist (recordid,timestamp, opcode, title, description, uuid,cover,author_id,post_num,follow_num,reply_num,elite,updated,created) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 1, NEW.title, NEW.description, NEW.uuid,NEW.cover,NEW.author_id,NEW.post_num,NEW.follow_num,NEW.reply_num,NEW.elite,NEW.updated,NEW.created) \
    ")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("DROP TRIGGER IF EXISTS synces_booklist_update")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("CREATE TRIGGER synces_booklist_update \
        AFTER UPDATE ON a2booklist \
        FOR EACH ROW \
        INSERT INTO synces_booklist (recordid,timestamp, opcode, title, description, uuid,cover,author_id,post_num,follow_num,reply_num,elite,updated,created) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 2, NEW.title, NEW.description, NEW.uuid,NEW.cover,NEW.author_id,NEW.post_num,NEW.follow_num,NEW.reply_num,NEW.elite,NEW.updated,NEW.created) \
    ")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("DROP TRIGGER IF EXISTS synces_booklist_delete")
    cursor.execute(manageBooklistTriggers)

    manageBooklistTriggers = ("CREATE TRIGGER synces_booklist_delete \
        BEFORE DELETE ON a2booklist \
        FOR EACH ROW \
        INSERT INTO synces_booklist (recordid,timestamp, opcode, title, description, uuid,cover,author_id,post_num,follow_num,reply_num,elite,updated,created) \
        VALUES (OLD.id, UNIX_TIMESTAMP(), 3, OLD.title, OLD.description, OLD.uuid,OLD.cover,OLD.author_id,OLD.post_num,OLD.follow_num,OLD.reply_num,OLD.elite,OLD.updated,OLD.created) \
    ")
    cursor.execute(manageBooklistTriggers)

    queryRecords = ("SELECT * FROM a2booklist")
    cursor.execute(queryRecords)
    records = cursor.fetchall()

    estype = "booklist"
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
                    'title':recordItem[2],
                    'description':recordItem[3],
                    'uuid':recordItem[1],
                    'cover':recordItem[4],
                    'author_id':recordItem[5],
                    'post_num':recordItem[6],
                    'follow_num':recordItem[7],
                    'reply_num':recordItem[8],
                    'elite':recordItem[9],
                    'updated':recordItem[10],
                    'created':recordItem[11]
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[0], body=doc)
                print('add/update booklist record %s => %s' %(recordItem[0],res))

    cnx.commit()
    cursor.close()
    cnx.close()

    return syncid_booklist

def job_booklist(syncid_booklist,dbuser,dbhost,dbpwd,dbname,es,esindex):
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname, connection_timeout=100)
    cursor = cnx.cursor()

    estype = "booklist"
    print("working on sync table booklist => use syncid %d" %syncid_booklist)

    querySync = ("SELECT * FROM synces_booklist WHERE id>"+str(syncid_booklist)+" ORDER BY id ASC")
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
                    'uuid':recordItem[6],
                    'cover':recordItem[7],
                    'author_id':recordItem[8],
                    'post_num':recordItem[9],
                    'follow_num':recordItem[10],
                    'reply_num':recordItem[11],
                    'elite':recordItem[12],
                    'updated':recordItem[13],
                    'created':recordItem[14]
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[1], body=doc)
                print('add/update booklist record %s => %s' %(recordItem[1],res))
            elif (recordItem[3] == 3): #delete
                try:
                    res = es.delete(index=esindex, doc_type=estype, id=recordItem[1])
                    print('delete doc %s => %s' %(recordItem[1],res))
                except NotFoundError, e:
                    print('doc %s found? %s' %(recordItem[1],e.info['found']))
        syncid_booklist = recordItem[0]
        print ("update sync id: %d" %syncid_booklist)

    cnx.commit()
    cursor.close()
    cnx.close()

    return syncid_booklist

