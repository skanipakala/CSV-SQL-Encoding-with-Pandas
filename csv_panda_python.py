import numpy as np
import pandas as pd
import pyodbc 
import csv

# merggin two csv files using pandas,...replace code with labels from lookup table
deptid = str('12345')

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=[SERVER NAME];"
                        "Database=[DB SCHEMA NAME];"
                        "uid=xxxx;pwd=xxxx;")
cursor = cnxn.cursor()
cursor.execute('''SELECT employee_name, employee_code from EMPLOYEE where dept_id=? ; ''' ,deptid)

# build df_code for codelist values and labels from SQL
columns = [ x[0] for x in cursor.description]
arr = []

for row in cursor.fetchall():
    cur_map = {}    
    for i in range(len(columns)):
        cur_map[columns[i]] = str(row[i])
    arr.append(cur_map)
    
df_codes = pd.DataFrame(arr)
if not df_codes.empty:
    unique_names = list(set(df_codes['employee_code'].tolist())) 

#close connection
cnxn.close()
    

#read original file with all data as string
df = pd.read_csv('C:\Project\INPUT.CSV', keep_default_na=False,dtype = str)

#function to find codelist label for value
def get_val(key, curr_column):
    if(key == ''):
        return key;
    matched_df = df_codes.loc[(df_codes['employee_code'] == curr_column) & (df_codes['value'] == key)]
    get_label = matched_df['label'].tolist()
    print (curr_column,key,get_label[0])
    if(len(get_label))>0:
        return get_label[0]
    else:
        return key
    
#process each column of original file      
for target in unique_names:
        if target in df.columns:
            #replace each codelist value with label
            df[target] = df[target].apply(lambda x: get_val(x,target))
            
#write updated CSV file with codelist labels
df.to_csv('C:\Project\decoded\OUTPUT.CSV', index = False,escapechar="\\",quoting=csv.QUOTE_ALL, encoding='utf-8',quotechar='"')