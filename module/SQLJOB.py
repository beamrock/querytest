import sys
import pandas as pd
import numpy as np
import module.DTFUNC        as DTFUNC
import module.EXCPTFUNC     as EXCPTFUNC

import module.SQLLIB        as SQLLIB

import pandas.io.sql as psql
from psycopg2 import OperationalError, errorcodes, errors

def COUNT_DELIETER(stmt,deli_symbol):
	cnt_deli=stmt.count(deli_symbol)
	return cnt_deli


def file_split(file_fname):
    list_file_fname = file_fname.split('.')
    file_name=list_file_fname[0]
    file_ext=list_file_fname[1]
    
    return file_name, file_ext
    

def CHK_TAB_EXISTS(conn,tablename):
    cur = conn.cursor()
    SQL_TAB_EXISTS=SQLLIB.EXIST_TAB % (tablename)
    cur.execute(SQL_TAB_EXISTS)
    SQL_TAB_EXISTS_RESULT=cur.fetchone()
    #print(SQL_TAB_EXISTS_RESULT[0])
    
    return SQL_TAB_EXISTS_RESULT[0]


###############################################
## Query the Meta Info
###############################################
def SELECT_COL_LIST(conn, tablename):
    sql_col_list=SQLLIB.SQL_COL_LIST % (tablename)
    df_col_list=psql.read_sql(sql_col_list, conn)
    
    str_col_list=','.join((df_col_list['column_name']))
    
    return df_col_list,str_col_list


###############################################
## Process the Dynamic SQL
###############################################
def EXEC_DYNAMIC_GDB(conn,cur):
    readsqllist="""select distinct cust1_tel_no, cust2_tel_no, clstr_ymd from cust_projection_3 order by clstr_ymd, cust1_tel_no, cust2_tel_no"""
    df_sqllist=psql.read_sql(readsqllist, conn)
    
    for i in range(len(df_sqllist)):
        cust1_tel_no=df_sqllist.iloc[i][0]
        cust2_tel_no=df_sqllist.iloc[i][1]
        clstr_ymd=df_sqllist.iloc[i][2]
        print(i, ' : ', clstr_ymd, cust1_tel_no, cust2_tel_no)
        
        cypher_shortest=f"""match p=shortestpath((a:vt_cust)-[e:edg_call_agg_day_pcc_sp{clstr_ymd}*]->(b:vt_cust))
        where a.tel_no = '{cust1_tel_no}'
          and b.tel_no = '{cust2_tel_no}'
        return length(p)"""
        cypher_shortest_str=SQL_CLS_SQ(cypher_shortest)
        
        start_time, start_time_str = DTFUNC.get_this_time()
        print ("start_time: ",start_time_str)     
        cur.execute(cypher_shortest)
        cypher_shortest_result=cur.fetchone()
        if isinstance(cypher_shortest_result, type(None)):
            cypher_shortest_result_val=0
        else:
            cypher_shortest_result_val=cypher_shortest_result[0]
        
        end_time, end_time_str = DTFUNC.get_this_time()
        print ("end_time: ",end_time_str)
        
        elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
        print ("elapsed: ",elapsed_time_str)
        
        print(start_time, end_time, elapsed_sec, elapsed_time_str)
        
        if cypher_shortest_result_val != 0:
            cypher_shortest_result_insert = f"""insert into pcc_result1_1 values('cypher','{clstr_ymd}','{cust1_tel_no}','{cust2_tel_no}'
            ,'{cypher_shortest_result_val}','{start_time_str}','{end_time_str}',{elapsed_time_str}', {elapsed_sec},'{cypher_shortest_str}')"""
            cur.execute(cypher_shortest_result_insert)
            conn.commit()
    
    cur.close()
    conn.close()
        

def EXEC_DYNAMIC_RDB(conn,cur):
    readsqllist="""select distinct cust1_tel_no, cust2_tel_no, clstr_ymd, node_length from pcc_result1_1 order by clstr_ymd, cust1_tel_no, cust2_tel_no"""
    df_sqllist=psql.read_sql(readsqllist, conn)
    
    for i in range(len(df_sqllist)):
        cust1_tel_no=df_sqllist.iloc[i][0]
        cust2_tel_no=df_sqllist.iloc[i][1]
        clstr_ymd=df_sqllist.iloc[i][2]
        node_length=int(df_sqllist.iloc[i][3])
        max_table_length=node_length-1
        str_node_length=str(node_length)
        
        print(i, ' : ', clstr_ymd, cust1_tel_no, cust2_tel_no, node_length )
        
        table_name = f"cdrhists_agg_pcc_sp{clstr_ymd}"
        table_alias = f"t{str_node_length}"
        table_next_alias =''
        add_from_table=''
        add_where_table=''
        add_from_table_sum=''
        add_where_table_sum=''
        j=1
        
        for j in range(node_length):
            table_alias=f't{j}'
            table_next_alias=f't{j+1}'
            add_from_table=f", {table_name} as {table_alias}"
            #print(add_from_table)
            add_from_table_sum=add_from_table_sum + ''.join(add_from_table)
            
        if j < node_length-1:
            add_where_table=f"and {table_alias}.in_tel_no_12digit = {table_next_alias}.out_tel_no_12digit "
            add_where_table_sum= add_where_table_sum + ''.join(add_where_table)
        #print(add_wher_table)
        #print("---------------------------")
    
    #print(add_from_table_sum)
    #print(add_where_table_sum)
    if node_length==1:
        sql_shortest=f"""select 1
  from cust c01
, cust c02
{add_from_table_sum}
where c01.tel_no_12digit = '{cust1_tel_no}'
  and c02.tel_no_12digit = '{cust2_tel_no}'
  and c01.tel_no_12digit = {table_alias}.out_tel_no_12digit
  and c02.tel_no_12digit = {table_alias}.in_tel_no_12digit
"""

    else:
        sql_shortest=f"""select 1
  from cust c01
, cust c02
{add_from_table_sum}
where c01.tel_no_12digit = '{cust1_tel_no}'
  and c02.tel_no_12digit = '{cust2_tel_no}'
  and c01.tel_no_12digit = t0.out_tel_no_12digit
  and c02.tel_no_12digit = t{max_table_length}.in_tel_no_12digit
  {add_where_table_sum}
"""
        
        sql_shortest_str=SQL_CLS_SQ(sql_shortest)
        
        start_time, start_time_str = DTFUNC.get_this_time()
        print ("start_time: ",start_time_str)     
        cur.execute(sql_shortest)
        sql_shortest_result=cur.fetchone()
        
        if isinstance(sql_shortest_result, type(None)):
            sql_shortest_result_val=0
        else:
            sql_shortest_result_val=sql_shortest_result[0]
        
        end_time, end_time_str = DTFUNC.get_this_time()
        print ("end_time: ",end_time_str)
        
        elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
        print ("elapsed: ",elapsed_time_str)
        
        print(start_time, end_time, elapsed_sec, elapsed_time_str)
        
        if sql_shortest_result_val != 0:
            sql_shortest_result_insert = f"""insert into pcc_result1_1_rdb values('cypher','{clstr_ymd}','{cust1_tel_no}','{cust2_tel_no}'
            ,'{sql_shortest_result_val}','{start_time_str}','{end_time_str}',{elapsed_time_str}', {elapsed_sec},'{sql_shortest_str}')"""
            cur.execute(sql_shortest_result_insert)
            conn.commit()
    
    cur.close()
    conn.close()
        

def LOAD_CD_LIST(conn, code_nm, code_tab):
    sql_cd_list="""select %s from %s""" % (code_nm,code_tab)
    df_cd_list = psql.read_sql(sql_cd_list, conn)
    return df_cd_list
    
def DDL_PART_RANGE(start,end,conn,proc_dt,table_nm):
    dates=DTFUNC.date_range(start,end)
    
    cursor = conn.cursor()
    for i in range(len(dates)-1):
        sql_cre_part="""CREATE TABLE %s_p%s PARTITION OF %s FOR VALUES FROM (%s) TO (%s) partition by list(pfix_cd) 
                        tablespace nastbs""" % (table_nm, dates[i], table_nm, dates[i], dates[i+1])
        print(sql_cre_part)
        try:
            cursor.execute(sql_cre_part)
            conn.commit()
        except:
            print("Already DDL")
            conn.rollback()
            continue
        
    print("Create Range Partition Complete")
    
    #Default SubPartition
    sql_cre_part_default="""CREATE TABLE %s_pdefault PARTITION OF %s default tablespace nastbs""" % (table_nm, table_nm)
    print(sql_cre_part_default)
    
    try:
        cursor.execute(sql_cre_part_default)
        conn.commit()
    except:
        print("Already DDL or the table does not exists.")
        conn.rollback()
    cursor.close()
    print("Create Default Range Partition Complete")
    
### CREATE LIST SUBPARTITION
def DDL_SUBPART_LIST(conn,proc_dt,table_nm):
    code_nm = "pfix_cd"
    code_tab = "pfix_cd_list"
    df_cd_list = LOAD_CD_LIST(conn, code_nm, code_tab)
    cursor = conn.cursor()
    
    for i in range(len(df_cd_list)):
        sql_cre_subpart="""CREATE TABLE %s_p%s_sp%s PARTITION OF %s_p%s FOR VALUES IN ('%s') tablespace nastbs""" 
        ddl_cre_subpart=sql_cre_subpart % (table_nm, proc_dt, df_cd_list.iloc[i][0], table_nm, proc_dt, df_cd_list.iloc[i][0])
        print(ddl_cre_subpart)
        
        try:
            cursor.execute(ddl_cre_subpart)
            conn.commit()
        except:
            print("Already DDL or the table does not exists.")
            conn.rollback()
            continue
        
    print("Create Range Partition Complete")
    
    #Default SubPartition
    sql_cre_part_default="""CREATE TABLE %s_pdefault PARTITION OF %s default tablespace nastbs""" % (table_nm, table_nm)
    print(sql_cre_part_default)
    
    try:
        cursor.execute(sql_cre_part_default)
        conn.commit()
    except:
        print("Already DDL or the table does not exists.")
        conn.rollback()
    cursor.close()
    print("Create Default Range Partition Complete")
 


###############################################
## DCL / DDL
###############################################
def SET_SESSION_PARAMETER(conn, cur, SESSION_PLAN_NUM, PARALLEL_NUM):
    if PARALLEL_NUM != 0:
        cur.execute(SQLLIB.SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_NUM)
        cur.execute(SQLLIB.SS_SET_MAX_PARALLEL_WORKERS_NUM)
    
    else:
        cur.execute(SQLLIB.SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_0)
        cur.execute(SQLLIB.SS_SET_MAX_PARALLEL_WORKERS_0)
    
    if SESSION_PLAN_NUM != 0:
        cur.execute(SQLLIB.SS_SET_HASHJOIN_OFF)
        cur.execute(SQLLIB.SS_SET_MERGJOIN_OFF)
        cur.execute(SQLLIB.SS_SET_NESTLOOP_OFF)
        
        if SESSION_PLAN_NUM == 1:
           cur.execute(SQLLIB.SS_SET_NESTLOOP_ON)
        elif SESSION_PLAN_NUM == 2:
           cur.execute(SQLLIB.SS_SET_HASHJOIN_ON)    
        elif SESSION_PLAN_NUM == 3:
           cur.execute(SQLLIB.SS_SET_MERGJOIN_ON)    
    
    else:
        print("Default Setting")

    df_paramlist=psql.read_sql(SQLLIB.PG_SET_PLAN_PARAMETER, conn)
    print(df_paramlist)


###############################################
## execute SQL
###############################################
def INSERT_SQLRESULT(save_cur,SQL_ID,SQL_GB,conn_alias,SQL_TEXT,SQL_TEXT_RESULT_TO_STR,input_param,start_time_str,end_time_str,elapsed_time_str,nowDate,REPEAT_NUM,PARALLEL_NUM,SESS_PLAN_NUM,SQL_TEXT_PLAN_BF,SQL_TEXT_PLAN_AF):
    # Cleansing SQL_TEXT
    exec_sql_snap=SQL_CLS_SQ(SQL_TEXT)
    DB_RESULT=SQL_CLS_SQ(SQL_TEXT_RESULT_TO_STR)
    execute_bf_plan=SQL_CLS_SQ(SQL_TEXT_PLAN_BF)
    execute_af_plan=SQL_CLS_SQ(SQL_TEXT_PLAN_AF)        
    
    INSERT_RESULT=f"INSERT INTO SQLRESULT VALUES('{SQL_ID}','{SQL_GB}','{conn_alias}','{exec_sql_snap}','{DB_RESULT}','{input_param}','{start_time_str}','{end_time_str}','{elapsed_time_str}','{nowDate}',{REPEAT_NUM},{PARALLEL_NUM},{SESS_PLAN_NUM},'{execute_bf_plan}','{execute_af_plan}')"
    save_cur.execute(INSERT_RESULT)
    #save_cur.close()
    
def EXECUTE_QUERY_WITHOUT_BIND(conn, cur, SQL_TEXT):
         try:   
             
            # By return as Dataframe
            start_time, start_time_str = DTFUNC.get_this_time()
            print ("start_time: ",start_time_str)
            df_SQL_TEXT_RESULT=psql.read_sql(SQL_TEXT, conn)
            SQL_TEXT_RESULT_TO_STR=df_SQL_TEXT_RESULT.to_string(index=True)
            #SQL_TEXT_RESULT_TO_STR=str(df_SQL_TEXT_RESULT.iloc[0][0])
            print(SQL_TEXT_RESULT_TO_STR)
            
            # By return as List
            #cur.execute(SQL_TEXT)
            #LIST_SQL_TEXT_RESULT=cur.fetchall()
            #print(type(LIST_SQL_TEXT_RESULT), LIST_SQL_TEXT_RESULT)
            #SQL_TEXT_RESULT_TO_STR = ', '.join(str(e) for e in LIST_SQL_TEXT_RESULT)
            #print(SQL_TEXT_RESULT_TO_STR)
            
            end_time, end_time_str = DTFUNC.get_this_time()
            #print ("end_time: ",end_time_str)
            elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
            #print ("elapsed: ",elapsed_time_str) 
            
            
            
            
            #SQL_TEXT_PLAN_BF=GET_PLAN_BF(SQL_TEXT, conn)
            #SQL_TEXT_PLAN_AF=GET_PLAN_AF(SQL_TEXT, conn)
            SQL_TEXT_PLAN_BF=''
            SQL_TEXT_PLAN_AF=''

            return SQL_TEXT_RESULT_TO_STR, SQL_TEXT_PLAN_BF, SQL_TEXT_PLAN_AF, start_time_str, end_time_str, elapsed_time_str


         except OperationalError as err:
            err, line_num, traceback, err_type, err_diag, err_pgerror, err_pgcode=EXCPTFUNC.print_psycopg2_exception(err)
            #print(err, line_num, traceback, err_type, err_diag, err_pgerror, err_pgcode)
    
            end_time, end_time_str = DTFUNC.get_this_time()
            #print ("end_time: ",end_time_str)
            
            elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
            #print ("elapsed: ",elapsed_time_str)  
            
            SQL_TEXT_RESULT='OperationalError'
            
            return SQL_TEXT_RESULT,'-','-',start_time_str,end_time_str,elapsed_time_str
                
         except:
             end_time, end_time_str = DTFUNC.get_this_time()
             #print ("end_time: ",end_time_str)
            
             elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
             #print ("elapsed: ",elapsed_time_str)  
            
             SQL_TEXT_RESULT='no rows'                    
    
             return SQL_TEXT_RESULT,'-','-',start_time_str,end_time_str,elapsed_time_str
           


def EXECUTE_NO_QUERY(conn, cur, SQL_TEXT):
    try:
        start_time, start_time_str = DTFUNC.get_this_time()
        #print ("start_time: ",start_time_str)
                            
        cur.execute(SQL_TEXT)
        conn.commit()
        
        end_time, end_time_str = DTFUNC.get_this_time()
        #print ("end_time: ",end_time_str)
        
        elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
        #print ("elapsed: ",elapsed_time_str)                    
        
        SQL_TEXT_RESULT='COMPLETE'
    
        return SQL_TEXT_RESULT,start_time_str,end_time_str,elapsed_time_str

    except OperationalError as err:
        err, line_num, traceback, err_type, err_diag, err_pgerror, err_pgcode=EXCPTFUNC.print_psycopg2_exception(err)
        #print(err, line_num, traceback, err_type, err_diag, err_pgerror, err_pgcode)

        end_time, end_time_str = DTFUNC.get_this_time()
        print ("end_time: ",end_time_str)
        
        elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
        print ("elapsed: ",elapsed_time_str)  
        
        SQL_TEXT_RESULT='OperationalError'
        
        return SQL_TEXT_RESULT,start_time_str,end_time_str,elapsed_time_str
            
    except:
        end_time, end_time_str = DTFUNC.get_this_time()
        print ("end_time: ",end_time_str)
        
        elapsed_sec, elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
        print ("elapsed: ",elapsed_time_str)  
        
        SQL_TEXT_RESULT='Unknown Error'                    

        return SQL_TEXT_RESULT,start_time_str,end_time_str,elapsed_time_str




###############################################
## EXCUTE PLAN
###############################################
# 사전 실행 계획
def GET_PLAN_BF(SQL_TEXT_RESULT_TO_STR, conn):
    SQL_PLAN_BF='explain ' + SQL_TEXT_RESULT_TO_STR
    df_PLAN_BF=psql.read_sql(SQL_PLAN_BF, conn)
    df_PLAN_BF_RESULT=df_PLAN_BF['QUERY PLAN'].astype(str)
    
    df_PLAN_BF_RESULT_TO_STR=""
    for k1 in range(len(df_PLAN_BF_RESULT)):
        df_PLAN_BF_RESULT_TO_STR+=df_PLAN_BF_RESULT.iloc[k1]+'\n'

    df_PLAN_BF_RESULT_TO_STR=SQL_CLS_SQ(df_PLAN_BF_RESULT_TO_STR)
    
    return df_PLAN_BF_RESULT_TO_STR

# 사후 실행 계획
def GET_PLAN_AF(SQL_TEXT_RESULT_TO_STR, conn):
    SQL_PLAN_AF='explain analyze ' + SQL_TEXT_RESULT_TO_STR
    df_PLAN_AF=psql.read_sql(SQL_PLAN_AF, conn)
    df_PLAN_AF_RESULT=df_PLAN_AF['QUERY PLAN'].astype(str)
    
    df_PLAN_AF_RESULT_TO_STR=""
    for k1 in range(len(df_PLAN_AF_RESULT)):
        df_PLAN_AF_RESULT_TO_STR+=df_PLAN_AF_RESULT.iloc[k1]+'\n'

    df_PLAN_AF_RESULT_TO_STR=SQL_CLS_SQ(df_PLAN_AF_RESULT_TO_STR)
    
    return df_PLAN_AF_RESULT_TO_STR        




def SQL_CLS_SQ(sql_text):
    SQL_TEXT2=sql_text.replace("'","''")
   #SQL_TEXT2=SQL_TEXT2.replace("/","//")    
    
    return SQL_TEXT2


# def SQLLIST_LOAD_EXCEL(conn,cur):

#     df_sqllist=pd.read_excel(EXCEL_FILE, sheet_name='2-0_SQL_Total')
#     df_sqllist['SQL_TEXT']=df_sqllist['SQL_TEXT'].replace(np.nan,'-')
#     df_sqllist['SQL_DESC']=df_sqllist['SQL_DESC'].replace(np.nan,'-')


def RESET_SQLRESULT_DIFF(conn,cur,tablename):
    SQL_TAB_EXISTS=SQLLIB.EXIST_TAB % (tablename)
    cur.execute(SQL_TAB_EXISTS)
    SQL_TAB_EXISTS_RESULT=cur.fetchone()
    
    if SQL_TAB_EXISTS_RESULT[0]==0:
        cur.execute(SQLLIB.CRE_SQLRESULT_DIFF)
        conn.commit()
    else:
        cur.execute(SQLLIB.DROP_SQLRESULT_DIFF)
        conn.commit()        
        cur.execute(SQLLIB.CRE_SQLRESULT_DIFF)
        conn.commit()
    
    print("Complete resetting the SQL Result Diff Table. ")


def RESET_SQLRESULT(conn,cur,tablename):
    SQL_TAB_EXISTS=SQLLIB.EXIST_TAB % (tablename)
    cur.execute(SQL_TAB_EXISTS)
    SQL_TAB_EXISTS_RESULT=cur.fetchone()
    
    if SQL_TAB_EXISTS_RESULT[0]==0:
        cur.execute(SQLLIB.SQLRESULT_CRE_TAB)
        conn.commit()
    else:
        cur.execute(SQLLIB.SQLRESULT_DROP_TAB)
        conn.commit()        
        cur.execute(SQLLIB.SQLRESULT_CRE_TAB)
        conn.commit()
    
    print("Complete resetting the SQL Result Table. ")


def LOAD_EXCEL(conn,cur,data_dir,filename):

    EXCEL_FILE=data_dir + '\\' + filename
    print(EXCEL_FILE)
    

    df01=pd.read_excel(EXCEL_FILE, sheet_name='2-0_SQL_Total')
    #print(df01.columns)
    df01['SQL_TEXT']=df01['SQL_TEXT'].replace(np.nan,'-')
    df01['SQL_DESC']=df01['SQL_DESC'].replace(np.nan,'-')
    
    for i in range(len(df01)):
        SQL_TEXT=df01.iloc[i][6].replace("'","''")
        SQL_DESC=df01.iloc[i][7].replace("'","''")
    
        try:
            SQL_LOAD=SQLLIB.SQLLIST_INSERT_ROW % ( df01.iloc[i][0], df01.iloc[i][1], df01.iloc[i][2], df01.iloc[i][3], df01.iloc[i][4], df01.iloc[i][5], SQL_TEXT, SQL_DESC, df01.iloc[i][8], df01.iloc[i][9], df01.iloc[i][10] )
            #print(SQL_LOAD)
            
            cur.execute(SQL_LOAD)
            conn.commit()
        except:
            print("Check the SQL_ID : " + df01.iloc[i][0])
            sys.exit()
            
    print("Complete Loading the SQL List Count : ", i)


    
def LOAD_SQLLIST(conn,cur,curr_dir,tablename,sql_file):
    SQL_TAB_EXISTS=SQLLIB.EXIST_TAB % (tablename)
    #print(SQL_TAB_EXISTS)    
    cur.execute(SQL_TAB_EXISTS)
    SQL_TAB_EXISTS_RESULT=cur.fetchone()
    #print(SQL_TAB_EXISTS_RESULT[0])
    
    if SQL_TAB_EXISTS_RESULT[0]==0:
        cur.execute(SQLLIB.SQLLIST_CRE_TAB)
        conn.commit()
    else:
        cur.execute(SQLLIB.SQLLIST_DROP_TAB)
        conn.commit()        
        cur.execute(SQLLIB.SQLLIST_CRE_TAB)
        conn.commit()    

    
    EXCEL_FILE=curr_dir + "\\import\\" + sql_file
    #EXCEL_FILE="C:/googledrive/비트나인/테스트/검증SQLList_20210909.xlsx"

    df01=pd.read_excel(EXCEL_FILE, sheet_name='AGE_Official')
    #print(df01.columns)
    df01['SQL_TEXT']=df01['SQL_TEXT'].replace(np.nan,'-')
    df01['SQL_DESC']=df01['SQL_DESC'].replace(np.nan,'-')
    
    for i in range(len(df01)):
        SQL_TEXT=df01.iloc[i][6].replace("'","''")
        SQL_DESC=df01.iloc[i][7].replace("'","''")
    
        try:
            SQL_LOAD=SQLLIB.SQLLIST_INSERT_ROW % ( df01.iloc[i][0], df01.iloc[i][1], df01.iloc[i][2], df01.iloc[i][3], df01.iloc[i][4], df01.iloc[i][5], SQL_TEXT, SQL_DESC, df01.iloc[i][8], df01.iloc[i][9], df01.iloc[i][10] )
            print(SQL_LOAD)
            
            cur.execute(SQL_LOAD)
            conn.commit()
        except:
            print("Check the SQL_ID : " + df01.iloc[i][0])
            sys.exit()
            
    print("Complete Loading the SQL List Count : ", i+1)


def LOAD_SQLPARAM(conn,cur,curr_dir,tablename,sql_file):

    SQL_TAB_EXISTS=SQLLIB.EXIST_TAB % (tablename)
    cur.execute(SQL_TAB_EXISTS)
    conn.commit()
    SQL_TAB_EXISTS_RESULT=cur.fetchone()
    #print(type(SQL_TAB_EXISTS_RESULT))
    #print(SQL_TAB_EXISTS_RESULT)
    #print(SQL_TAB_EXISTS_RESULT[0])
    #str1=''.join(SQL_TAB_EXISTS_RESULT)::int
    #print(str1)
    
    if SQL_TAB_EXISTS_RESULT[0]==0:
        cur.execute(SQLLIB.SQLPARAM_CRE_TAB)
        conn.commit()
    else:
        cur.execute(SQLLIB.SQLPARAM_DROP_TAB)
        conn.commit()        
        cur.execute(SQLLIB.SQLPARAM_CRE_TAB)
        conn.commit()     

    EXCEL_FILE=curr_dir + "\\import\\" + sql_file
    #EXCEL_FILE="C:/googledrive/비트나인/테스트/검증SQLList_20210909.xlsx"
    
    df02=pd.read_excel(EXCEL_FILE, sheet_name='params')
    
    df02['param01']=df02['param01'].replace(np.nan,'-')
    df02['param02']=df02['param02'].replace(np.nan,'-')
    df02['param03']=df02['param03'].replace(np.nan,'-')
    df02['param04']=df02['param04'].replace(np.nan,'-')
    df02['param05']=df02['param05'].replace(np.nan,'-')
    df02['param06']=df02['param06'].replace(np.nan,'-')
    df02['param07']=df02['param07'].replace(np.nan,'-')
    df02['param08']=df02['param08'].replace(np.nan,'-')
    df02['param09']=df02['param09'].replace(np.nan,'-')
    df02['param10']=df02['param10'].replace(np.nan,'-')
        
    for j in range(len(df02)):
    
        try:
            SQL_LOAD=SQLLIB.SQLPARAM_INSERT_ROW % ( df02.iloc[j][0], df02.iloc[j][1], df02.iloc[j][2], df02.iloc[j][3], df02.iloc[j][4], df02.iloc[j][5], df02.iloc[j][6], df02.iloc[j][7], df02.iloc[j][8], df02.iloc[j][9], df02.iloc[j][10] )
            #print(SQL_LOAD)
            
            cur.execute(SQL_LOAD)
            conn.commit()
        except:
            print("Check the SQL_ID : " + df02.iloc[j][0])
            sys.exit()

    print("Complete Loading the SQL Params Count : ", j+1)


def CRE_SGI_META_CLS01(conn, cur):
    
    sql_svc_3_nm_like_keyword="""select STRNG_AGG(T.keyword, '|') from sgi_meta_dict_except t where col_nm = 'svc_3_nm' and except_type = 'LIKE'"""
    cur.execute(sql_svc_3_nm_like_keyword)
    svc_3_nm_like_keyword=cur.fetchone()[0]
    print(svc_3_nm_like_keyword)
    print("")

    sql_domain_nm_like_keyword="""select STRNG_AGG(T.keyword, '|') from sgi_meta_dict_except t where col_nm = 'domain_nm' and except_type = 'LIKE'"""
    cur.execute(sql_domain_nm_like_keyword)
    domain_nm_like_keyword=cur.fetchone()[0]
    print(domain_nm_like_keyword)
    print("")
    
    cur.execute("TRUNCATE TABLE sgi_meta_cls01")
    
    start_time, start_time_str = DTFUNC.get_this_time()
    print ("start_time: ",start_time_str)
                        
    sql_cre_sgi_meta_cls01="""insert into sgi_meta_cls01
    select t2.*
         , t2.domain_nm_array[cardinality(domain_nm_array)] as domain_last_keyword
      from (
           select distinct
                  svc_1_nm
                , svc_2_nm
                , svc_3_nm
                , case when srvr_nm = '_' and rqt_host_adr = '_' then host_dns
                       else concat(srvr_nm, '_', ''), replace(rqt_host_adr, '_', ''))
                  end as domain_nm
             from sgi_hist
            where svc_1_nm NOT IN ( select keyword from sgi_meta_dict_except) WHERE col_nm = 'svc_1_nm' )
              and svc_2_nm NOT IN ( select keyword from sgi_meta_dict_except) WHERE col_nm = 'svc_2_nm' )
              and svc_3_nm NOT IN ( select keyword from sgi_meta_dict_except) WHERE col_nm = 'svc_3_nm' )
              and svc_3_nm !~ '%s'
              and replace(srvr_nm, '_', '') || host_dns || replace(rqt_host_adr, '_', '') !~ '%s'
            order by 1,2,3,4 ) t1 ) t2 """ % (svc_3_nm_like_keyword, domain_nm_like_keyword)
    
    cur.execute(sql_cre_sgi_meta_cls01)
    conn.commit()
          
    end_time, end_time_str = DTFUNC.get_this_time()
    print ("end_time: ",end_time_str)
    
    elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
    print ("elapsed: ",elapsed_time_str)                    

def CRE_SGI_HIST_CLS01(conn, cur):
    
    sql_svc_3_nm_like_keyword="""select STRNG_AGG(T.keyword, '|') from sgi_meta_dict_except t where col_nm = 'svc_3_nm' and except_type = 'LIKE'"""
    cur.execute(sql_svc_3_nm_like_keyword)
    svc_3_nm_like_keyword=cur.fetchone()[0]
    print(svc_3_nm_like_keyword)
    print("")

    sql_domain_nm_like_keyword="""select STRNG_AGG(T.keyword, '|') from sgi_meta_dict_except t where col_nm = 'domain_nm' and except_type = 'LIKE'"""
    cur.execute(sql_domain_nm_like_keyword)
    domain_nm_like_keyword=cur.fetchone()[0]
    print(domain_nm_like_keyword)
    print("")
    
    cur.execute("TRUNCATE TABLE sgi_hist_cls01")
    
    sql_cre_sgi_hist_cls01="""insert into sgi_hist_cls01
    select t.*
         , t2.domain_nm_array[cardinality(domain_nm_array)] as domain_last_keyword
         , case when srvr_nm = '_' and rqt_host_adr = '_' then host_dns
                else concat(srvr_nm, '_', ''), replace(rqt_host_adr, '_', ''))
           end as domain_nm
     from sgi_hist
    where svc_1_nm NOT IN ( select keyword from sgi_meta_dict_except) WHERE col_nm = 'svc_1_nm' )
      and svc_2_nm NOT IN ( select keyword from sgi_meta_dict_except) WHERE col_nm = 'svc_2_nm' )
      and svc_3_nm NOT IN ( select keyword from sgi_meta_dict_except) WHERE col_nm = 'svc_3_nm' )
      and svc_3_nm !~ '%s'
      and replace(srvr_nm, '_', '') || host_dns || replace(rqt_host_adr, '_', '') !~ '%s'
    order by 1,2,3,4 ) t1 ) t2 """ % (svc_3_nm_like_keyword, domain_nm_like_keyword)    
    
    start_time, start_time_str = DTFUNC.get_this_time()
    print ("start_time: ",start_time_str)

    cur.execute(sql_cre_sgi_hist_cls01)
    conn.commit()
          
    end_time, end_time_str = DTFUNC.get_this_time()
    print ("end_time: ",end_time_str)
    
    elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
    print ("elapsed: ",elapsed_time_str)                            


def CRE_SGI_HIST_ORI_CLS01(conn, cur):
    
    cur.execute("TRUNCATE TABLE sgi_hist_ori_cls01")
    
    start_time, start_time_str = DTFUNC.get_this_time()
    print ("start_time: ",start_time_str)
    
    sql_cre_sgi_hist_cls01="""insert into sgi_hist_ori_cls01
    select t.*
         , case when t.srvr_nm = '_' and t.rqt_host_adr = '_' then host_dns
                else concat(replace(t.srvr_nm, '_', ''), replace(t.rqt_host_adr, '_', ''))
            end as domain_nm
      from sgi_hist t
    ORDER BY alc_svc_no_sorc_id, rqt_st_dt"""
    
    cur.execute(sql_cre_sgi_hist_cls01)
    conn.commit()
    
    end_time, end_time_str = DTFUNC.get_this_time()
    print ("end_time: ",end_time_str)
    
    elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
    print ("elapsed: ",elapsed_time_str)       


def CRE_SGI_HIST_CLS02(conn, cur):
    
    cur.execute("TRUNCATE TABLE sgi_hist_cls02")   

    start_time, start_time_str = DTFUNC.get_this_time()
    print ("start_time: ",start_time_str)
    
    sql_domain_nm_accept="""select t.domain, t.domain_key, t.svc_nm, t.app_nm from sgi_meta_dict_accept t"""
    df_domainlist=psql.read_sql(sql_domain_nm_accept, conn)
    
    for i in range(len(df_domainlist)):
        domain_nm_meta=df_domainlist.iloc[i][0]
        domain_key=df_domainlist.iloc[i][1]
        svc_nm=df_domainlist.iloc[i][2]
        app_nm=df_domainlist.iloc[i][3].replace("'","''")
        
        INSERT_RESULT="""INSERT INTO sgi_hist_cls02
                         SELECT t.*
                              , '%s'
                              , '%s'
                              , similarity(domain_nm, '%s')
                              , case when '%s' = '_' then '%s' else '%s' end as svc_nm
                           FROM sgi_hist_cls01 t
                          WHERE domain_nm like '%%%s%%'""" % (domain_nm_meta, domain_key, domain_nm_meta, svc_nm, app_nm, svc_nm, domain_key)
        
        print(i, " : ", domain_nm_meta, domain_key, domain_nm_meta, svc_nm, app_nm, svc_nm, domain_key)
        
        cur.execute(INSERT_RESULT)
        conn.commit()
        
    end_time, end_time_str = DTFUNC.get_this_time()
    print ("end_time: ",end_time_str)
        
    elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
    print ("elapsed: ",elapsed_time_str)
    

def cre_edg_domain_key_elem(conn, cur):
    cur.execute("DROP   ELABEL IF EXISTS edg_domain_key_elem CASCADE")
    cur.execute("CREATE ELABEL edg_domain_key_elem")
    cur.execute("ALTER  ELABEL edg_domain_key_elem SET UNLOGGED")

    start_time, start_time_str = DTFUNC.get_this_time()
    print ("start_time: ",start_time_str)

    sql_domain_keyword_dict="""select t.domain_key, t.domain_nm, t.elem, t.position_num, t.domain_key_cls, t.domain_key_num, t.domain_max_num 
                                 from domain_keyword_dict t"""
    df_domain_keyword_dict=psql.read_sql(sql_domain_keyword_dict, conn)

    CRE_EDGE_ELEM_DOMAIN_KEY="""MATCH (a1:vt_elem), (b1:vt_domain_key)
                                WHERE a1.elem = '%s'
                                  AND b1.domain_key = '%s'
                                MERGE (a1)-[e1:edg_domain_key_elem]->(b1)"""

    CRE_EDGE_ELEM_DOMAIN_KEY="""MATCH (a1:vt_domain_key), (b1:vt_elem) 
                                WHERE a1.domain_key = '%s' 
                                  AND b1.elem = '%s'
                                MERGE (a1)-[e1:edg_domain_key_elem]->(b1)"""
                                 
    CRE_EDGE_ELEM_ELEM="""MATCH (b1:vt_elem), (c1:vt_elem)
                          WHERE b1.elem = '%s'
                            AND c1.elem = '%s'
                          MERGE (a1)-[e1:edg_domain_key_elem]->(b1)"""
                          
    sql_search_elem = """select elem
                           from domain_keyword_dict
                          where 0=0
                            and domain_key = '%s'
                            and domain_nm  = '%s'
                            and position_num = '%s'"""
                            
    for i in range(len(sql_domain_keyword_dict)):
        domain_key=sql_domain_keyword_dict.iloc[i][0]
        domain_nm=sql_domain_keyword_dict.iloc[i][1]
        elem=sql_domain_keyword_dict.iloc[i][2]
        position_num=sql_domain_keyword_dict.iloc[i][3]
        domain_key_cls=sql_domain_keyword_dict.iloc[i][4]
        domain_key_num=sql_domain_keyword_dict.iloc[i][5]
        
        print(i, " : ", domain_key, domain_nm, elem, position_num, domain_key_cls, domain_key_num)
        
        v_position_num=0
        
        if elem==domain_key_cls:
            continue
        
        elif position_num < domain_key_num:
            v_position_num=position_num+1
        
            sql_search_next_elem = sql_search_elem % (domain_key, domain_nm, v_position_num)
            df_search_next_elem=psql.read_sql(sql_search_next_elem, conn)
            next_elem = df_search_next_elem.iloc[0][0]
            
            if v_position_num == domain_key_num:
                INSERT_RESULT=CRE_EDGE_ELEM_DOMAIN_KEY % (elem, domain_key)
                cur.execute(INSERT_RESULT)
                conn.commit()
            else:
                INSERT_RESULT=CRE_EDGE_ELEM_ELEM % (elem, next_elem)
                cur.execute(INSERT_RESULT)
                conn.commit()

        elif position_num > domain_key_num:
            v_position_num=position_num-1
        
            sql_search_elem = sql_search_elem % (domain_key, domain_nm, v_position_num)
            df_search_before_elem=psql.read_sql(sql_search_elem, conn)
            before_elem = df_search_before_elem.iloc[0][0]
            
            if v_position_num == domain_key_num:
                INSERT_RESULT=CRE_EDGE_DOMAIN_KEY_ELEM % (domain_key, elem)
                cur.execute(INSERT_RESULT)
                conn.commit()
            else:
                INSERT_RESULT=CRE_EDGE_ELEM_ELEM % (before_elem, elem)
                cur.execute(INSERT_RESULT)
                conn.commit()
        
        else:
            continue
        
        print(INSERT_RESULT)

    end_time, end_time_str = DTFUNC.get_this_time()
    print ("end_time: ",end_time_str)
        
    elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
    print ("elapsed: ",elapsed_time_str)
    
def cre_edge_connhist_connhist(conn, cur):
    cur.execute("DROP   ELABEL IF EXISTS edg_connhist_conhist CASCADE")
    cur.execute("CREATE ELABEL edg_connhist_conhist")
    cur.execute("ALTER  ELABEL edg_connhist_conhist SET UNLOGGED")

    start_time, start_time_str = DTFUNC.get_this_time()
    print ("start_time: ",start_time_str)
    
    sql_sgi_hist_cls02_sample01_cls01="""select t.* from sgi_hist_cls02_sample01_cls01 t"""
    df_sql_sgi_hist_cls02_sample01_cls01=psql.read_sql(sql_sgi_hist_cls02_sample01_cls01, conn)
    
    CRE_EDGE_connhist_connhist="""MATCH (a1:vt_connhist),(b1:vt_connhist)
                                  WHERE a1.rownum = %s
                                    AND b1.rownum = %s
                                 CREATE (a1)-[e1:edge_connhist_connhist]->(b1)"""
    
    for i in range(len(df_sql_sgi_hist_cls02_sample01_cls01)):
        rownum=df_sql_sgi_hist_cls02_sample01_cls01.iloc[i][0]
        after_rownum=rownum+1
        print("")
        
        if i < len(df_sql_sgi_hist_cls02_sample01_cls01):
            INSERT_RESULT=CRE_EDGE_connhist_connhist % ( rownum, after_rownum )
            cur.execute(INSERT_RESULT)
            conn.commit()
            print(INSERT_RESULT)
        else:
            continue

    end_time, end_time_str = DTFUNC.get_this_time()
    print ("end_time: ",end_time_str)
        
    elapsed_time_str = DTFUNC.get_elapsed_time(start_time,end_time)
    print ("elapsed: ",elapsed_time_str)
    

def execute(sql, params={}):
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)

            

    
    