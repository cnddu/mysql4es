import time
import json
import sys
import mysql.connector
from datetime import datetime

def init_tag(esindex,es,syncid_tag,dbuser,dbhost,dbpwd,dbname):
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname)
    cursor = cnx.cursor()

    #init for tag table
    manageTagTriggers = ("DROP TABLE IF EXISTS synces_tag")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("CREATE TABLE synces_tag (\
      id int(11) NOT NULL AUTO_INCREMENT,\
      recordid int(11),\
      timestamp int unsigned,\
      opcode tinyint,\
      name longtext,\
      english longtext,\
      description longtext,\
      avatar longtext,\
      cover longtext,\
      homepage longtext,\
      tag_type longtext,\
      parent_id bigint(20),\
      category_id bigint(20),\
      point_to bigint(20),\
      hidden tinyint(1),\
      post_num bigint(20),\
      follow_num bigint(20),\
      star_num bigint(20),\
      PRIMARY KEY (id)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("DROP TRIGGER IF EXISTS synces_tag_insert")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("CREATE TRIGGER synces_tag_insert \
        AFTER INSERT ON a2tag \
        FOR EACH ROW \
        INSERT INTO synces_tag (recordid,timestamp, opcode, name, english, description,avatar,cover,homepage,tag_type,parent_id,category_id,point_to,hidden,post_num,follow_num,star_num) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 1, NEW.name, NEW.english, NEW.description, NEW.avatar,NEW.cover,NEW.homepage,NEW.tag_type,NEW.parent_id,NEW.category_id,NEW.point_to,NEW.hidden,NEW.post_num,NEW.follow_num,NEW.star_num) \
    ")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("DROP TRIGGER IF EXISTS synces_tag_update")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("CREATE TRIGGER synces_tag_update \
        AFTER UPDATE ON a2tag \
        FOR EACH ROW \
        INSERT INTO synces_tag (recordid,timestamp, opcode, name, english, description,avatar,cover,homepage,tag_type,parent_id,category_id,point_to,hidden,post_num,follow_num,star_num) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 2, NEW.name, NEW.english, NEW.description, NEW.avatar,NEW.cover,NEW.homepage,NEW.tag_type,NEW.parent_id,NEW.category_id,NEW.point_to,NEW.hidden,NEW.post_num,NEW.follow_num,NEW.star_num) \
    ")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("DROP TRIGGER IF EXISTS synces_tag_delete")
    cursor.execute(manageTagTriggers)

    manageTagTriggers = ("CREATE TRIGGER synces_tag_delete \
        BEFORE DELETE ON a2tag \
        FOR EACH ROW \
        INSERT INTO synces_tag (recordid,timestamp, opcode, name, english, description,avatar,cover,homepage,tag_type,parent_id,category_id,point_to,hidden,post_num,follow_num,star_num) \
        VALUES (OLD.id, UNIX_TIMESTAMP(), 3, OLD.name, OLD.english, OLD.description,OLD.avatar,OLD.cover,OLD.homepage,OLD.tag_type,OLD.parent_id,OLD.category_id,OLD.point_to,OLD.hidden,OLD.post_num,OLD.follow_num,OLD.star_num) \
    ")
    cursor.execute(manageTagTriggers)

    queryRecords = ("SELECT * FROM a2tag")
    cursor.execute(queryRecords)
    records = cursor.fetchall()

    estype = "tag"
    res = es.indices.exists_type(index=esindex,doc_type=estype)
    print('check type exists: %s => %s' %(estype,res))
    if (res == False):
        mappingstr = {
            estype: {
                "_all": {
                    "enabled": "false"
                },
                "properties": {
                    "name": {
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
                    "english": {
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
                    'name':recordItem[1],
                    'english':recordItem[2],
                    'description':recordItem[3],
                    'avatar':recordItem[4],
                    'cover':recordItem[5],
                    'homepage':recordItem[6],
                    'tag_type':recordItem[7],
                    'parent_id':recordItem[8],
                    'category_id':recordItem[9],
                    'point_to':recordItem[10],
                    'hidden':recordItem[11],
                    'post_num':recordItem[12],
                    'follow_num':recordItem[13],
                    'star_num':recordItem[14]
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[0], body=doc)
                print('add/update tag record %s => %s' %(recordItem[0],res))

    cnx.commit()
    cursor.close()
    cnx.close()

    return syncid_tag

def job_tag(syncid_tag,dbuser,dbhost,dbpwd,dbname,es,esindex):
    cnx = mysql.connector.connect(user=dbuser, host=dbhost, password=dbpwd, database=dbname, connection_timeout=100)
    cursor = cnx.cursor()

    estype = "tag"
    print("working on sync table tag => use syncid %d" %syncid_tag)

    querySync = ("SELECT * FROM synces_tag WHERE id>"+str(syncid_tag)+" ORDER BY id ASC")
    cursor.execute(querySync)
    records = cursor.fetchall()
    print (records)

    if (len(records) > 0):
        for recordItem in records:
            print(recordItem)
            if (recordItem[3] == 1) or (recordItem[3] == 2): #insert
                doc = {
                    'id':recordItem[1],
                    'name':recordItem[4],
                    'english':recordItem[5],
                    'description':recordItem[6],
                    'avatar':recordItem[7],
                    'cover':recordItem[8],
                    'homepage':recordItem[9],
                    'tag_type':recordItem[10],
                    'parent_id':recordItem[11],
                    'category_id':recordItem[12],
                    'point_to':recordItem[13],
                    'hidden':recordItem[14],
                    'post_num':recordItem[15],
                    'follow_num':recordItem[16],
                    'star_num':recordItem[17]
                }
                res = es.index(index=esindex, doc_type=estype, id=recordItem[1], body=doc)
                print('add/update tag record %s => %s' %(recordItem[1],res))
            elif (recordItem[3] == 3): #delete
                try:
                    res = es.delete(index=esindex, doc_type=estype, id=recordItem[1])
                    print('delete doc %s => %s' %(recordItem[1],res))
                except NotFoundError, e:
                    print('doc %s found? %s' %(recordItem[1],e.info['found']))
        syncid_tag = recordItem[0]
        print ("update sync id: %d" %syncid_tag)

    cnx.commit()
    cursor.close()
    cnx.close()

    return syncid_tag

