
import pandas as pd
import numpy as np
import os
import time
import pyodbc
import uuid
import datetime
import jinja2
import pkg_resources
import importlib_resources



def sqlServer_loader():

    try:
        return pyodbc.connect(driver='ODBC Driver 17 for SQL Server', 
                              server='us6sdwn00150.wmservice.corpnet1.com', 
                              database='ToV',
                              user='opso_owner', 
                              password='N369KB#DA40#Vr59')
                              
    except Exception as e: 
        print(e)
        time.sleep(20)

def data_type(x):
    if x == 'varchar':
        return 'object'
    elif x == 'decimal':
        return 'float32'
    elif x == 'int':
        return 'int32'
    else: return 'object'

def loader(file, direc, table_id, rown):
    my_dict = {}

    for ind in dfc[dfc.table_id==table_id].index:
        my_dict[dfc['column_name'][ind]] = dfc['dt'][ind]

    ftype = str.lower(file[file.rfind('.') + 1:])
    print(file)
    if ftype == 'csv':
        return pd.read_csv(direc + '\\' + file, header=rown, dtype=my_dict)
    elif ftype == 'xlsx':
        return pd.read_excel(direc + '\\' + file, header=rown, dtype=my_dict)
    else:
        return 'Invalid File Type'
    
def combiner(direc, pattern, output, table_id, rown=0):

    files_list = os.listdir(direc)
    load_file = []

    
    if rown == -1:
        rown = None
    
    for i in pattern:
        load_file = list(set(load_file + [j for j in files_list if j.find(i) > -1]))

    if len(load_file) > 0:
        df = loader(load_file[0], direc, table_id, rown)
        df.to_csv(direc + '\\' + output + '_' + time.strftime ("%Y_%m_%d") + '.csv',index=False)
        
        for i in load_file[1:]:
            df = loader(i, direc, table_id, rown)
            df.to_csv(direc + '\\' + output + '_' + time.strftime ("%Y_%m_%d") + '.csv',index=False, mode='a', header=False)
            
        if os.path.isdir(direc + '\\Processed') == False:
            os.makedirs(direc + '\\Processed')
            
        for f in load_file:
            src_path = os.path.join(direc, f)
            dst_path = os.path.join(direc + '\\Processed', f)
            os.rename(src_path, dst_path)
            
        insert = 'INSERT INTO OpsO.conductor.action_history (id, run_date, conductor_id, success_flag, message) values(?,?,?,?,?)'
        cursor.execute(insert,[id,datetime.datetime.now(),95,1,str(dfi['output_file'][ind]) + ' created'])
        cursor.commit()




try:
    conn = sqlServer_loader()

    query = """SELECT 
t.location_url, c.pattern,
c.output_file, c.row_start, c.table_id
FROM 
OpsO.conductor.db_file_load_table t
INNER JOIN OpsO.conductor.db_file_combine c
on (t.id = c.table_id) """

    dfi = pd.read_sql(query, con=conn)

    query2 = """SELECT *
FROM 
OpsO.conductor.db_file_combine_column c"""

    dfc = pd.read_sql(query2, con=conn)
    dfc['dt'] = dfc.data_type.apply(lambda x: data_type(x))

except Exception as e: 
    print(e)
    time.sleep(20)

try:
    cursor = conn.cursor()
except Exception as e: 
    print(e)
    time.sleep(20)

id = cursor.execute('select max(id) + 1 from opso.conductor.action_history').fetchall()[0][0]

insert = 'INSERT INTO OpsO.conductor.action_history (id, run_date, conductor_id, success_flag, message) values(?,?,?,?,?)'
cursor.execute(insert,[id,datetime.datetime.now(),95,1,'file_combiner.exe started run'])
cursor.commit()

id = id + 1

for ind in dfi.index:
    try:
        combiner(dfi['location_url'][ind], dfi['pattern'][ind].split(',')
                 , dfi['output_file'][ind], dfi['table_id'][ind], dfi['row_start'][ind])

    except Exception as e: 
        insert = 'INSERT INTO OpsO.conductor.action_history (id, run_date, conductor_id, success_flag, message) values(?,?,?,?,?)'
        cursor.execute(insert,[id,datetime.datetime.now(),95,0,e])
        cursor.commit()
        
    id = id + 1

insert = 'INSERT INTO OpsO.conductor.action_history (id, run_date, conductor_id, success_flag, message) values(?,?,?,?,?)'
cursor.execute(insert,[id,datetime.datetime.now(),95,1,'file_combiner.exe completed run'])
cursor.commit()

cursor.close()





    
