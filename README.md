# mysql-cloudera.hive-replication
Replication of mysql binlogs to hive
below are the packages for replication

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication
import csv
import boto3
import mysql.connector
from mysql.connector import Error
import pymysql
import collections
from collections import Counter
import sys
from impala.dbapi import connect
from hdfs import InsecureClient

while True:
    conn = pymysql.connect(host="xxxxx",user="xxxxxa",password="password",port=3306,db="xxxxxta")
    mysql_settings = {'host': '54.xxxxx250',
                      'port': 3306,
                      'user': 'cxxxxxa',
                      'passwd': 'paxxxxxd'
                     }
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=100,
        only_events=[row_event.UpdateRowsEvent]
    )

    log_upd_events = []
    for binlogevent in stream:
        for row in binlogevent.rows:
            if binlogevent.table == 'texxxxxa':
                upd_event = {}
                if isinstance(binlogevent, row_event.UpdateRowsEvent):
                    upd_event["action"] = "update"
                    upd_event["table"] = binlogevent.table
                    upd_event["database"] = binlogevent.schema
                    upd_event.update(row["after_values"].items())
                    # print(row["after_values"].items())
                    log_upd_events.append(upd_event)
                    # print(upd_event)
    stream.close()
    changed = log_upd_events
    # print(changed)
    if (len(changed) > 0):
        for events_iteration in changed:
            if events_iteration['action'] == 'update':
                dict_value = events_iteration.copy()
                tablename = dict_value['table']
                database = dict_value['database']
                # after_values = dict_value[]
                if (database == 'CD_data'):
                    dict_value.pop('action')
                    dict_value.pop('table')
                    dict_value.pop('database')
                    set_placeholder = ""
                    where_placeholder = ""
                    for key, value in row["after_values"].items():
                        if value is None:
                            set_key_value = str(key) + " = 'Null'"
                        else:
                            set_key_value = str(key) + " = '" + str(value) + "'"
                            # set_key_value = str(key)+" = '"+str(value)+"'"
                            set_placeholder = set_placeholder + " , " + set_key_value if set_placeholder != "" else set_key_value
                    for key, value in row["before_values"].items():
                        if value is None:
                            where_key_value = str(key) + " = 'Null'"
                        else:
                            where_key_value = str(key) + " = '" + str(value) + "'"
                        where_placeholder = where_placeholder + " AND " + where_key_value if where_placeholder != "" else where_key_value
                    hive = "UPDATE %s SET %s WHERE  %s " % (tablename, set_placeholder, where_placeholder)
                    print(hive)
                    conn = connect(host='54.xxxxx9', port=10000,
                                   auth_mechanism='PLAIN',
                                   user='hive',
                                   password='hive',
                                   database='Cxxxxxa')
                    cursor = conn.cursor()
                    cursor.execute(hive)
                    conn.commit()
                    conn.close()
                    print("Update Operation")


For delete events replication:::::

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication
import csv
import boto3
import mysql.connector
from mysql.connector import Error
import pymysql
import collections
from collections import Counter
import sys
from impala.dbapi import connect
from hdfs import InsecureClient

while True:
    conn = pymysql.connect(host="54xxxxx.250", user="cldera", password="password", port=3306, db="CD_DATA")
    mysql_settings = {'host': '54.xxxx.250',
                      'port': 3306,
                      'user': 'cxxxxx',
                      'passwd': 'password'
                      }
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=100,
        only_events=[row_event.DeleteRowsEvent]
    )

    log_events = []
    for binlogevent in stream:
        for row in binlogevent.rows:
            if binlogevent.table == 'testdata':
                del_event = {}
                if isinstance(binlogevent, row_event.DeleteRowsEvent):
                    del_event["action"] = "delete"
                    del_event["table"] = binlogevent.table
                    del_event["database"] = binlogevent.schema
                    del_event.update(row["values"].items())
                    log_events.append(del_event)
    stream.close()
    change = log_events
    if (len(change) > 0):
        for event_iteration in change:
            if event_iteration['action'] == 'delete':
                dict_value = event_iteration.copy()
                tablename = dict_value['table']
                database = dict_value['database']
                if (database == 'cldera'):
                    dict_value.pop('action')
                    dict_value.pop('table')
                    dict_value.pop('database')
                    placeholder = ""
                    for key, value in dict_value.items():
                        if value is None:
                            key_value = str(key) + " = 'Null'"
                        else:
                            key_value = str(key) + " = '" + str(value) + "'"
                        placeholder = placeholder + " AND " + key_value if placeholder != "" else key_value
                    hive = "DELETE FROM %s WHERE  %s " % (tablename, placeholder)
                    conn = connect(host='34xxxx1.91', port=10000,
                                   auth_mechanism='PLAIN',
                                   user='hive',
                                   password='hive',
                                   database='CDxxxxA')
                    cursor = conn.cursor()
                    cursor.execute(hive)
                    print(hive)
                    conn.commit()
                    print("Delete Operation")
                    cursor.close()
                    conn.close()


