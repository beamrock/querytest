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

def session_setting(parallel_num, sess_plan_num):
    ######################################
    # Setting Session Parameters
    ######################################
    SS_SET_HASHJOIN_OFF="""set enable_hashjoin='off'"""
    SS_SET_MERGJOIN_OFF="""set enable_mergejoin='off'"""
    SS_SET_NESTLOOP_OFF="""set enable_nestloop='off'"""

    SS_SET_NESTLOOP_ON="""set enable_nestloop='on'"""
    SS_SET_HASHJOIN_ON="""set enable_hashjoin='on'"""
    SS_SET_MERGJOIN_ON="""set enable_mergejoin='on'"""


    SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_0="""set max_parallel_workers_per_gather='0'"""
    SS_SET_MAX_PARALLEL_WORKERS_0="""set max_parallel_workers='0'"""

    SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_NUM="""set max_parallel_maintenance_workers='%s'""" % (parallel_num)
    SS_SET_MAX_PARALLEL_WORKERS_NUM="""set max_parallel_workers='%s'""" % (parallel_num)
    
    if sess_plan_num != 0:
        curA.execute(SS_SET_HASHJOIN_OFF)
        curA.execute(SS_SET_MERGJOIN_OFF)
        curA.execute(SS_SET_NESTLOOP_OFF)
        
        if sess_plan_num == 1:
           curA.execute(SS_SET_NESTLOOP_ON)
        elif sess_plan_num == 2:
           curA.execute(SS_SET_HASHJOIN_ON)    
        elif sess_plan_num == 3:
           curA.execute(SS_SET_MERGJOIN_ON)    
    
    else:
        print("Default Setting")

    if parallel_num != 0:
        curA.execute(SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_NUM)
        curA.execute(SS_SET_MAX_PARALLEL_WORKERS_NUM)
    
    else:
        curA.execute(SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_0)
        curA.execute(SS_SET_MAX_PARALLEL_WORKERS_0)

    paramlist="""select name, setting 
                   from pg_settings where name in (
                       'enable_nestloop'
                      ,'enable_hashjoin'
                      ,'enable_mergejoin'
                      ,'max_parallel_workers_per_gather'
                      ,'max_parallel_workers'
                      )"""

    df_paramlist=psql.read_sql(paramlist, connA)
    print(df_paramlist)
    