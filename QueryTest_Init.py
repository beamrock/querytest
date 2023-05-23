# -*- coding: utf-8 -*-
##########################################################################################
### Declare Import Libraries
# Concerned with Default Setting
from psycopg2 import __version__ as psycopg2_version
print ("psycopg2 version:", psycopg2_version, "\n")
import os

# User-Define Module
import module.SQLJOB        as SQLJOB
import module.DBCONN        as DBCONN
import module.SQLLIB        as SQLLIB
#import module.SETPARAMFUNC  as SETPARAMFUNC
##########################################################################################

##########################################################################################
##########################################################################################
### Main
if __name__ == '__main__':    

    ##########################################################################################
    ## DB Connect
    df_dbconn_info=DBCONN.DBCONN_INFO()
    print(df_dbconn_info)
    
    compDB_A = 'conn_age_ldbc'
    conn, df_conndb=DBCONN.PG_DBCONN_TRY01(df_dbconn_info, compDB_A)
    
    ##########################################################################################
    ### Default Setting
    curr_dir=os.getcwd()
    print(curr_dir)
    
    cur=conn.cursor()
    SQLJOB.LOAD_SQLLIST(conn, cur, curr_dir, 'sqllist','QueryList.xlsx')
    SQLJOB.LOAD_SQLPARAM(conn, cur, curr_dir, 'sqlparam','QueryList.xlsx')
    SQLJOB.RESET_SQLRESULT(conn, cur, 'sqlresult')
    SQLJOB.RESET_SQLRESULT_DIFF(conn, cur, 'sqlresult_diff')
    cur.close()
