# -*- coding: utf-8 -*-
##########################################################################################
### Declare Import Libraries
## Concerned with Default Setting
import datetime
import time
import sys
import os
#os.chdir('/root/script/BM_Process/module')
#os.chdir('D:\\DaumCloud\\1.AWS\\BigData_Python\\BM_Process\\module')

## import the connect library for psycopg2
from psycopg2 import connect
import psycopg2 as pg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import __version__ as psycopg2_version
#print ("psycopg2 version:", psycopg2_version, "\n")

## Concerned with Data Analysis Process
import pandas as pd
import pandas.io.sql as psql
#import numpy as np

import module.EXCPTFUNC     as EXCPTFUNC

import module.SQLLIB        as SQLLIB


def DBCONN_INFO():
    # DBLIST
    # db01=pd.Series(['ldbc_ag12','snb100gb','192.168.16.85','5500','snbuser','1234','ldbc_graph'])
    # db02=pd.Series(['ldbc_ag11','snb100gb','192.168.16.85','5432','snbuser','1234','ldbc_graph'])
    # db03=pd.Series(['ldbc_ag10','snb100gb','192.168.16.85','5434','snbuser','1234','ldbc_graph'])
    # db04=pd.Series(['ldbc_ag10','snb100gb','192.168.16.85','5434','snbuser','1234','ldbc_graph'])
    # db05=pd.Series(['ldbc_ag10','snb100gb','192.168.16.85','5434','snbuser','1234','ldbc_graph'])
    # db06=pd.Series(['ldbc_ag10','snb100gb','192.168.16.85','5434','snbuser','1234','ldbc_graph'])
    #db07=pd.Series(['kyowon_ag12','knowledgespace','192.168.0.15','5432','kyowon','1234','ks_graph'])
    #db08=pd.Series(['kyowon_ag11','knowledgespace','192.168.0.16','5432','kyowon','1234','ks_graph'])
    #df_dblist=pd.DataFrame([list(db01),list(db02),list(db03),list(db04),list(db05),list(db06),list(db07),list(db08)], columns=['CONN_ALIAS','DB_NM','IP','PORT','USER_NM','PASSWORD','GRAPH_NM'])
    
    db01=pd.Series(['ag21_lnx','agensgraph','emaildb','192.168.0.12','5432','bitnine','1234','mail_graph'])
    db02=pd.Series(['conn_age_movie','age'     ,'moviedb','192.168.54.52' ,'15411' ,'bitnine','1234','movie_graph'])
    #db03=pd.Series(['conn_age'  ,'age'       ,'sampledb','192.168.54.52' ,'15411','bitnine','1234','ldbc_graph'])
    #db04=pd.Series(['conn_ag'   ,'agensgraph','sampledb','192.168.54.52' ,'15432','bitnine','1234','movie_graph'])
    db03=pd.Series(['conn_age_ldbc','age'       ,'snb1gb','192.168.54.52' ,'15411','bitnine','1234','ldbc_graph'])
    db04=pd.Series(['conn_ag'   ,'agensgraph','snb1gb','192.168.54.52' ,'15432','bitnine','1234','ldbc_graph'])
    db05=pd.Series(['networkx','agensgraph','sampledb','192.168.16.6','35432','bitnine','1234','movie_graph'])
    db06=pd.Series(['ag250'   ,'agensgraph','snb1gb','192.168.54.52','5432','agens','1234','ldbc_graph'])
    df_dblist=pd.DataFrame([list(db01),list(db02),list(db03),list(db04),list(db05),list(db06)], columns=['CONN_ALIAS','DBMS_NM','DB_NM','IP','PORT','USER_NM','PASSWORD','GRAPH_NM'])
    #print(df_dblist)
   
    return df_dblist

def AGE_DBCONN_TRY01(df_dblist, connDB):
    #connDB_A = input("Input the CONN_ALIAS of first DB compared ?")
    #connDB_A = 'ldbc_ag11'
    print(df_dblist)
    df_conndb = df_dblist[(df_dblist['CONN_ALIAS']==connDB)]
    print(df_conndb)
    
    print("[Connect AGE]")
    print("CONN_ALIAS : " + df_conndb.iloc[0,0])
    print("DBMS_NM    : " + df_conndb.iloc[0,1])
    print("DB_NM      : " + df_conndb.iloc[0,2])
    print("IP         : " + df_conndb.iloc[0,3])
    print("PORT       : " + df_conndb.iloc[0,4])
    print("USER_NM    : " + df_conndb.iloc[0,5])
    print("PASSWORD   : " + df_conndb.iloc[0,6])
    print("Default AGE Graph : " + df_conndb.iloc[0,7])
      
    # Try Connect
    try:
        print("Connecting DB")
        conn=pg2.connect(dbname=df_conndb.iloc[0,2], host=df_conndb.iloc[0,3],port=df_conndb.iloc[0,4], user=df_conndb.iloc[0,5],password=df_conndb.iloc[0,6])    
        conn.autocommit = True
        print("Current autocommit Setting : ", conn.autocommit)
        print("connected to the DB.")
        print("")
       
        # Initial Session
        try:
            cur=conn.cursor()
            DB_A_SET_GRAPH="""SET search_path = ag_catalog, "$user", public, '%s'""" % (df_conndb.iloc[0,7])
            cur.execute(DB_A_SET_GRAPH)
            conn.commit()
            cur.close()
            print("Complete set the graph")            
        except:
            print("Not Set")
            cur.close()
        
        return conn, df_conndb
        
    except OperationalError as err:
        EXCPTFUNC.print_psycopg2_exception(err)
        sys.exit()
        

def PG_DBCONN_TRY01(df_dblist, connDB):
    #connDB_A = input("Input the CONN_ALIAS of first DB compared ?")
    #connDB_A = 'ldbc_ag11'
    df_conndb = df_dblist[(df_dblist['CONN_ALIAS']==connDB)]
    print(df_conndb)
    
    print("[Connect AgensGraph]")
    print("CONN_ALIAS : " + df_conndb.iloc[0,0])
    print("DBMS_NM    : " + df_conndb.iloc[0,1])
    print("DB_NM      : " + df_conndb.iloc[0,2])
    print("IP         : " + df_conndb.iloc[0,3])
    print("PORT       : " + df_conndb.iloc[0,4])
    print("USER_NM    : " + df_conndb.iloc[0,5])
    print("PASSWORD   : " + df_conndb.iloc[0,6])
    print("Default AgensGraph Graph : " + df_conndb.iloc[0,7])
      
    # Try Connect
    try:
        print("Connecting DB")
        conn=pg2.connect(dbname=df_conndb.iloc[0,2], host=df_conndb.iloc[0,3],port=df_conndb.iloc[0,4], user=df_conndb.iloc[0,5],password=df_conndb.iloc[0,6])    
        conn.autocommit = True
        print("Current autocommit Setting : ", conn.autocommit)
        print("connected to the DB.")
        print("")
       
        # Initial Session
        try:
            cur=conn.cursor()
            DB_A_SET_GRAPH="""set graph_path='%s'""" % (df_conndb.iloc[0,7])
            cur.execute(DB_A_SET_GRAPH)
            conn.commit()
            cur.close()
            print("Complete set the graph")
        except:
            print("Not  Set Graph")
            cur.close()
        
        return conn, df_conndb
        
    except OperationalError as err:
        EXCPTFUNC.print_psycopg2_exception(err)
        sys.exit()
        

def PG_DBCONN_TRY02(params_dic):
    
    try:
        conn=pg2.connect(**params_dic)
        print("Connection Successful")
        
        return conn

    except OperationalError as err:
        EXCPTFUNC.print_psycopg2_exception(err)
        sys.exit()
  