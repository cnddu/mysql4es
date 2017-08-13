import time
import mysql.connector
from datetime import datetime

cnx = mysql.connector.connect(user='root', host='121.42.235.121', password='lovepan_0808', database='love2io')
cursor = cnx.cursor()

queryRecords = ("SELECT username FROM user")
cursor.execute(queryRecords)
records = cursor.fetchall()
if (records is not None):
    fpList = []
    for recordItem in records:
        fpList.append(recordItem)
        print(recordItem)

cnx.commit()
cursor.close()
cnx.close()
