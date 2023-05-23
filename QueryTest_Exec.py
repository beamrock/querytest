# import sys to get more detailed Python exception info
import sys
import json
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import OperationalError, errorcodes, errors
# User-Define Module
import module.DBCONN        as DBCONN
import module.DTFUNC        as DTFUNC
import module.EXCPTFUNC     as EXCPTFUNC
import module.SQLJOB        as SQLJOB
#import module.SETPARAMFUNC  as SETPARAMFUNC
import os
##########################################################################################

##########################################################################################
### Default Setting
curr_dir=os.getcwd()

## DB Connect
df_dbconn_info=DBCONN.DBCONN_INFO()
print(df_dbconn_info)

list_conn_alias=['conn_age_ldbc']

df_target_db_info = df_dbconn_info.query('CONN_ALIAS in @list_conn_alias')
print(df_target_db_info)

list_conn_ag=[]
list_conn_age=[]
df_target_ag=[]
df_target_age=[]

for i in range(len(df_target_db_info)):
    if df_target_db_info.iloc[i][1] == 'agensgraph' :
        conn_ag, df_ag=DBCONN.PG_DBCONN_TRY01(df_target_db_info, df_target_db_info.iloc[i][0])
        list_conn_ag.append(conn_ag)
        df_target_ag.append(df_ag)

    elif df_target_db_info.iloc[i][1] == 'age':
        conn_age, df_age=DBCONN.AGE_DBCONN_TRY01(df_target_db_info, df_target_db_info.iloc[i][0])
        list_conn_age.append(conn_age)
        df_target_age.append(df_age)

print("Target AG  DB : ", list_conn_ag)
print("Target AGE DB : ", list_conn_age)

# Dedicate the DB of Result Table Creating 
if len(list_conn_ag) != 0:
    conn_save_result = list_conn_ag[0]
else:
    conn_save_result = list_conn_age[0]    

cur_save_result = conn_save_result.cursor()
print("SAVE DB : ", conn_save_result)

## Load SQL List By Table
#readsqllist="""SELECT * FROM SQLLIST where conn_alias in ('agensgraph','agedb','age') and sql_id like 'MOV%' and sql_id not in ('MOV001')  order by sql_id, dbms_type desc limit 3"""
#readsqllist="""SELECT * FROM SQLLIST where dbms_type in ('agensgraph','age') and sql_id like 'MOV002' and sql_id not in ('MOV001') order by sql_id, dbms_type desc"""
#readsqllist="""SELECT * FROM sqllist where SQL_GB = 'READ' and sql_usage = 'ldbc_graph' order by sql_id, dbms_type desc"""
readsqllist="""SELECT * FROM sqllist where SQL_GB = 'READ' and sql_usage = 'ldbc_graph' order by sql_id, dbms_type desc"""

try:
    df_sqllist=psql.read_sql(readsqllist, conn_save_result)
except:
    print("Can't Load the SQL List")
    sys.exit(1)

## Execute SQL on Each DB and Save the Result
exec_date=DTFUNC.nowDate
exec_time=DTFUNC.now

df_sqlresult = pd.DataFrame(columns=['sql_id', 'sql_gb', 'conn_alias', 'exec_sql_snap','result','input_param','time_start','time_end','time_elapsed','exec_date','repeat_num','parallel_num','sess_plan_num','execute_bf_plan','esecute_af_plan'])

for i in range(len(df_sqllist)):
    # Declare Executing SQL Info     
    SQL_ID        = df_sqllist.iloc[i][0]    
    SQL_GB        = df_sqllist.iloc[i][2]    
    BIND_YN       = df_sqllist.iloc[i][4]    
    DBMS_TYPE     = df_sqllist.iloc[i][5]
    SQL_TEXT      = df_sqllist.iloc[i][6]
    REPEAT_NUM    = int(df_sqllist.iloc[i][8])
    PARALLEL_NUM  = df_sqllist.iloc[i][9]
    SESS_PLAN_NUM = df_sqllist.iloc[i][10]    # 0 : Affect DB Level Parameter  # 1 : Only enable_nestloop=on  # 2 : Only enable_hashjoin=on  # 3 : Only enable_mergejoin=on      
  
    print("")
    print("SQL ID : ", SQL_ID)
    print(DBMS_TYPE, SQL_TEXT)

    LIST_DB_RESULT=[]
    DICT_DB_RESULT={}
    print()
    print("SQL Type(QUERY/DCL/DML/DDL) : ", SQL_GB)
    
    
    #############################
    ## On AgensGraph
    #############################    
    if DBMS_TYPE == 'agensgraph':
        
        for j in range(len(list_conn_ag)):
            conn_alias = 'agensgraph'
            conn = list_conn_ag[j]
            cur=conn.cursor()
        
            if SQL_GB in ('DCL','DML','DDL'):
                input_param='-'; SQL_TEXT_PLAN_BF='-'; SQL_TEXT_PLAN_AF='-'
                SQL_TEXT_RESULT_TO_STR,start_time_str,end_time_str,elapsed_time_str = SQLJOB.EXECUTE_NO_QUERY(conn, cur, SQL_TEXT)
                SQLJOB.INSERT_SQLRESULT(cur_save_result,SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF)
                new_row = [SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF]
                df_sqlresult.loc[len(df_sqlresult)] = new_row
            
            elif SQL_GB=='READ':
                SQLJOB.SET_SESSION_PARAMETER(conn, cur, SESS_PLAN_NUM, PARALLEL_NUM)
                print("Repeat Count : ", REPEAT_NUM)
                for w in range(REPEAT_NUM):
                    print ("Repeat : ", w+1, " 번째" )
                    
                    if BIND_YN == 'N':
                        input_param='-'
                        SQL_TEXT_RESULT_TO_STR, SQL_TEXT_PLAN_BF, SQL_TEXT_PLAN_AF, start_time_str, end_time_str, elapsed_time_str = SQLJOB.EXECUTE_QUERY_WITHOUT_BIND(conn, cur, SQL_TEXT)
                        SQLJOB.INSERT_SQLRESULT(cur_save_result,SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF)
                        new_row = [SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF]
                        df_sqlresult.loc[len(df_sqlresult)] = new_row
                        
                    
                    elif BIND_YN != 'N':
                        sql_paramlist=f"select * from sqlparam where sql_id = '{BIND_YN}'"
                        df_paramlist=psql.read_sql(sql_paramlist, conn_save_result)
                        
                        print("")
                        print("Bind Variables Count :", len(df_paramlist))
                        print("")
                        for k1 in range(len(df_paramlist)): 
                            param_num=int(df_paramlist.iloc[k1][1])
                            
                            param_array=[]
                            input_param=""
                            for k2 in range(param_num):
                                param_array.append(df_paramlist.iloc[k1][k2+2])
                               
                                if k2 == 0 or k2 == param_num:
                                    input_param+=df_paramlist.iloc[k1][k2+2]
                                else:
                                    input_param+=','+df_paramlist.iloc[k1][k2+2]
                                   
                            SQL_TEXT_AFFECT_BIND=SQL_TEXT.format(*param_array)
                            
                            SQL_TEXT_RESULT_TO_STR, SQL_TEXT_PLAN_BF, SQL_TEXT_PLAN_AF, start_time_str, end_time_str, elapsed_time_str = SQLJOB.EXECUTE_QUERY_WITHOUT_BIND(conn, cur, SQL_TEXT_AFFECT_BIND)
                            SQLJOB.INSERT_SQLRESULT(cur_save_result,SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF)
                            new_row = [SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF]
                            df_sqlresult.loc[len(df_sqlresult)] = new_row
                            
                            
            cur.close()                    

    #############################
    ## On AGE
    #############################     
    if DBMS_TYPE.lower() == 'age' or DBMS_TYPE.lower() == 'agedb' :
        
        for j in range(len(list_conn_age)):
            conn_alias = 'age'
            conn = list_conn_age[j]
            cur=conn.cursor()
        
            if SQL_GB in ('DCL','DML','DDL'):
                input_param='-'; SQL_TEXT_PLAN_BF='-'; SQL_TEXT_PLAN_AF='-'
                SQL_TEXT_RESULT_TO_STR,start_time_str,end_time_str,elapsed_time_str = SQLJOB.EXECUTE_NO_QUERY(conn, cur, SQL_TEXT)
                SQLJOB.INSERT_SQLRESULT(cur_save_result,SQL_ID,SQL_GB,DBMS_TYPE,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF)
                new_row = [SQL_ID,SQL_GB,DBMS_TYPE,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF]
                df_sqlresult.loc[len(df_sqlresult)] = new_row

            elif SQL_GB=='READ':
                SQLJOB.SET_SESSION_PARAMETER(conn, cur, SESS_PLAN_NUM, PARALLEL_NUM)
                print("Repeat Count : ", REPEAT_NUM)
                for w in range(REPEAT_NUM):
                    print ("Repeat : ", w+1, " 번째" )
                    
                    if BIND_YN == 'N':
                        input_param='-'
                        SQL_TEXT_RESULT_TO_STR, SQL_TEXT_PLAN_BF, SQL_TEXT_PLAN_AF, start_time_str, end_time_str, elapsed_time_str = SQLJOB.EXECUTE_QUERY_WITHOUT_BIND(conn, cur, SQL_TEXT)
                        SQLJOB.INSERT_SQLRESULT(cur_save_result,SQL_ID,SQL_GB,DBMS_TYPE,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF)
                        new_row = [SQL_ID,SQL_GB,DBMS_TYPE,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF]
                        df_sqlresult.loc[len(df_sqlresult)] = new_row


                    elif BIND_YN != 'N':
                        sql_paramlist=f"select * from sqlparam where sql_id = '{BIND_YN}'"
                        df_paramlist=psql.read_sql(sql_paramlist, conn_save_result)
                        
                        print("")
                        print("Bind Variables Count :", len(df_paramlist))
                        print("")
                        for k1 in range(len(df_paramlist)): 
                            param_num=int(df_paramlist.iloc[k1][1])
                            
                            param_array=[]
                            input_param=""
                            for k2 in range(param_num):
                                param_array.append(df_paramlist.iloc[k1][k2+2])
                               
                                if k2 == 0 or k2 == param_num:
                                    input_param+=df_paramlist.iloc[k1][k2+2]
                                else:
                                    input_param+=','+df_paramlist.iloc[k1][k2+2]
                                   
                            SQL_TEXT_AFFECT_BIND=SQL_TEXT.format(*param_array)
                            SQL_TEXT_RESULT_TO_STR, SQL_TEXT_PLAN_BF, SQL_TEXT_PLAN_AF, start_time_str, end_time_str, elapsed_time_str = SQLJOB.EXECUTE_QUERY_WITHOUT_BIND(conn, cur, SQL_TEXT_AFFECT_BIND)
                            SQLJOB.INSERT_SQLRESULT(cur_save_result,SQL_ID,SQL_GB,DBMS_TYPE,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF)
                            new_row = [SQL_ID,SQL_GB,DBMS_TYPE,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,exec_date,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF]
                            df_sqlresult.loc[len(df_sqlresult)] = new_row

            cur.close()                    

    ################################
    ## Processing the Result Diff
    ################################     
    df_result_sql_id = df_sqlresult.query('sql_id in @SQL_ID')

    diff_cnt=0
    
    LIST_DB_RESULT=[]
    DICT_DB_RESULT={}
    DICT_SQL={}
    if len(df_result_sql_id) > 1:
        
        for a in range(len(df_result_sql_id)-1):
            if a == 0:
                DICT_DB_RESULT[df_result_sql_id.iloc[a][2]]=df_result_sql_id.iloc[a][4]
                DICT_SQL[df_result_sql_id.iloc[a][2]]=SQLJOB.SQL_CLS_SQ(df_result_sql_id.iloc[a][3])
            
            if df_result_sql_id.iloc[a][4] != df_result_sql_id.iloc[a+1][4]:
                diff_cnt=+1
                DICT_DB_RESULT[df_result_sql_id.iloc[a+1][2]]=df_result_sql_id.iloc[a+1][4]
                DICT_SQL[df_result_sql_id.iloc[a+1][2]]=SQLJOB.SQL_CLS_SQ(df_result_sql_id.iloc[a+1][3])
        
        if diff_cnt != 0:
            #print(DICT_DB_RESULT)
            json_DB_RESULT=SQLJOB.SQL_CLS_SQ(json.dumps(DICT_DB_RESULT))
            json_DB_SQL=SQLJOB.SQL_CLS_SQ(json.dumps(DICT_SQL))
            #exec_sql_snap=SQLJOB.SQL_CLS_SQ(SQL_TEXT)
            INSERT_RESULT_DIFF=f"INSERT INTO SQLRESULT_DIFF VALUES('{SQL_ID}','{json_DB_SQL}','{json_DB_RESULT}','-','{exec_date}','{exec_time}','{input_param}')"
            print(INSERT_RESULT_DIFF)
    
            cur_save_result.execute(INSERT_RESULT_DIFF)
            conn_save_result.commit()
            #cur_save_result.close()


print("-------------------")
print("Finish Execute SQL.")
print("-------------------")
