# -*- coding: utf-8 -*-

import sqlite3
import json

connection = sqlite3.connect('emitters.db')

cursor = connection.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS emitters "
               "(id INTEGER PRIMARY KEY, name TEXT NOT NULL, risk TEXT)")


fh = open('emitters.json','r', encoding="utf-8")
emitters_list = json.loads(fh.read())
fh.close()

for entry in emitters_list:
    sql_query = "INSERT INTO emitters (id, name, risk) SELECT '{0}','{1}','{2}' " \
                "WHERE NOT EXISTS(SELECT 1 FROM emitters WHERE id = {0})".format(entry[0], entry[1], entry[2])
    # print(sql_query)
    cursor.execute(sql_query)

connection.commit()
connection.close()
