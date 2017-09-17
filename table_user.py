import time
import json
import sys
import mysql.connector
from datetime import datetime

def init_user(esindex,es,syncid_user,dbuser,dbhost,dbpwd,dbname):
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
      username longtext,\
      nickname longtext,\
      avatar longtext,\
      description longtext,\
      `key` longtext,\
      level int(11),\
      reg_time bigint(20),\
      key_time bigint(20),\
      position longtext,\
      company longtext,\
      homepage longtext,\
      city longtext,\
      star_num bigint(20),\
      view_num bigint(20),\
      docs_num bigint(20),\
      stars_num bigint(20),\
      following_num bigint(20),\
      followers_num bigint(20),\
      booklist_num bigint(20),\
      followbooklist_num bigint(20),\
      PRIMARY KEY (id)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("DROP TRIGGER IF EXISTS synces_user_insert")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TRIGGER synces_user_insert \
        AFTER INSERT ON user \
        FOR EACH ROW \
        INSERT INTO synces_user (recordid,timestamp, opcode, username, nickname, avatar, description, `key`,level,reg_time,key_time,position,company,homepage,city,star_num,view_num,docs_num,stars_num,following_num,followers_num,booklist_num,followbooklist_num) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 1, NEW.username, NEW.nickname, NEW.avatar, NEW.description, NEW.key,NEW.level,NEW.reg_time,NEW.key_time,NEW.position,NEW.company,NEW.homepage,NEW.city,NEW.star_num,NEW.view_num,NEW.docs_num,NEW.stars_num,NEW.following_num,NEW.followers_num,NEW.booklist_num,NEW.followbooklist_num) \
    ")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("DROP TRIGGER IF EXISTS synces_user_update")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TRIGGER synces_user_update \
        AFTER UPDATE ON user \
        FOR EACH ROW \
        INSERT INTO synces_user (recordid,timestamp, opcode, username, nickname, avatar, description, `key`,level,reg_time,key_time,position,company,homepage,city,star_num,view_num,docs_num,stars_num,following_num,followers_num,booklist_num,followbooklist_num) \
        VALUES (NEW.id, UNIX_TIMESTAMP(), 2, NEW.username, NEW.nickname, NEW.avatar, NEW.description, NEW.key,NEW.level,NEW.reg_time,NEW.key_time,NEW.position,NEW.company,NEW.homepage,NEW.city,NEW.star_num,NEW.view_num,NEW.docs_num,NEW.stars_num,NEW.following_num,NEW.followers_num,NEW.booklist_num,NEW.followbooklist_num) \
    ")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("DROP TRIGGER IF EXISTS synces_user_delete")
    cursor.execute(manageUserTriggers)

    manageUserTriggers = ("CREATE TRIGGER synces_user_delete \
        BEFORE DELETE ON user \
        FOR EACH ROW \
        INSERT INTO synces_user (recordid,timestamp, opcode, username, nickname, avatar, description,`key`,level,reg_time,key_time,position,company,homepage,city,star_num,view_num,docs_num,stars_num,following_num,followers_num,booklist_num,followbooklist_num) \
        VALUES (OLD.id, UNIX_TIMESTAMP(), 3, OLD.username, OLD.nickname, OLD.avatar, OLD.description,OLD.key,OLD.level,OLD.reg_time,OLD.key_time,OLD.position,OLD.company,OLD.homepage,OLD.city,OLD.star_num,OLD.view_num,OLD.docs_num,OLD.stars_num,OLD.following_num,OLD.followers_num,OLD.booklist_num,OLD.followbooklist_num) \
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
            #try:
            #    docexist = True
            #    res = es.get(index=esindex, doc_type=estype, id=recordItem[0])
            #    print('search for user record id: %s => %s' %(recordItem[0],res))
            #except NotFoundError, e:
            #    docexist = e.info['found']
            #time.sleep(10000);
            doc = {
                'userid':recordItem[0],
                'username':recordItem[1],
                'nickname':recordItem[7],
                'avatar':recordItem[2],
                'description':recordItem[10],
                'key':recordItem[3],
                'level':recordItem[4],
                'reg_time':recordItem[5],
                'key_time':recordItem[6],
                'position':recordItem[8],
                'company':recordItem[9],
                'homepage':recordItem[11],
                'city':recordItem[12],
                'star_num':recordItem[13],
                'view_num':recordItem[14],
                'docs_num':recordItem[15],
                'stars_num':recordItem[16],
                'following_num':recordItem[17],
                'followers_num':recordItem[18],
                'booklist_num':recordItem[19],
                'followbooklist_num':recordItem[20]
            }
            res = es.index(index=esindex, doc_type=estype, id=recordItem[0], body=doc)
            print('add/update user record %s => %s' %(recordItem[0],res))

    cnx.commit()
    cursor.close()
    cnx.close()

    return syncid_user

def job_user(syncid_user,dbuser,dbhost,dbpwd,dbname,es):
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
                    'key':recordItem[8],
                    'level':recordItem[9],
                    'reg_time':recordItem[10],
                    'key_time':recordItem[11],
                    'position':recordItem[12],
                    'company':recordItem[13],
                    'homepage':recordItem[14],
                    'city':recordItem[15],
                    'star_num':recordItem[16],
                    'view_num':recordItem[17],
                    'docs_num':recordItem[18],
                    'stars_num':recordItem[19],
                    'following_num':recordItem[20],
                    'followers_num':recordItem[21],
                    'booklist_num':recordItem[22],
                    'followbooklist_num':recordItem[23]
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

    return syncid_user
