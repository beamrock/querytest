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


######################################
# Setting Session Parameters
######################################
SS_SET_HASHJOIN_OFF="""set enable_hashjoin='off'"""
SS_SET_MERGJOIN_OFF="""set enable_mergejoin='off'"""
SS_SET_NESTLOOP_OFF="""set enable_nestloop='off'"""

SS_SET_NESTLOOP_ON="""set enable_nestloop='on'"""
SS_SET_HASHJOIN_ON="""set enable_hashjoin='on'"""
SS_SET_MERGJOIN_ON="""set enable_mergejoin='on'"""


EXIST_TAB="""SELECT EXISTS (select tablename from pg_tables where tablename = lower('%s'))::int"""

SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_0="""set max_parallel_workers_per_gather='0'"""
SS_SET_MAX_PARALLEL_WORKERS_0="""set max_parallel_workers='0'"""

SS_SET_MAX_PARALLEL_WORKERS_PER_GATHER_NUM="""set max_parallel_maintenance_workers='%s'"""
SS_SET_MAX_PARALLEL_WORKERS_NUM="""set max_parallel_workers='%s'"""


PG_SET_PLAN_PARAMETER="""select name, setting 
from pg_settings 
where name in (
 'enable_nestloop'
,'enable_hashjoin'
,'enable_mergejoin'
,'max_parallel_workers_per_gather'
,'max_parallel_workers'
)"""



TRUNC_SQLRESULT="""TRUNCATE TABLE SQLRESULT"""
DRP_SQLRESULT="""DROP TABLE IF EXISTS SQLRESULT"""
CRE_SQLRESULT="""CREATE IF NOT EXISTS TABLE SQLRESULT(
  SQL_ID VARCHAR
, CONN_ALIAS VARCHAR
, TIME_START VARCHAR
, TIME_END VARCHAR
, TIME_ELAPSED VARCHAR
, EXEC_DATE VARCHAR
, EXEC_SQL_SNAP TEXT
, INPUT_PARAM VARCHAR
, RESULT TEXT
, EXECUTE_BF_PLAN TEXT
, EXECUTE_AF_PLAN TEXT
)"""

TRUNC_SQLRESULT_DIFF="""TRUNCATE TABLE SQLRESULT_DIFF"""
DROP_SQLRESULT_DIFF="""DROP TABLE IF EXISTS SQLRESULT_DIFF"""
CRE_SQLRESULT_DIFF="""CREATE TABLE IF NOT EXISTS SQLRESULT_DIFF(
  SQL_ID VARCHAR
, EXEC_SQL_SNAP TEXT
, RESULT JSON
, DIFF_REASON TEXT
, EXEC_DATE VARCHAR
, EXEC_TIME VARCHAR
, INPUT_PARAM VARCHAR
)"""


SQLRESULT_TRUNC_TAB="""TRUNCATE TABLE SQLRESULT"""

SQLRESULT_DROP_TAB="""DROP TABLE IF EXISTS  SQLRESULT"""

SQLRESULT_CRE_TAB="""CREATE TABLE IF NOT EXISTS SQLRESULT(
  SQL_ID VARCHAR
, SQL_GB VARCHAR
, CONN_ALIAS VARCHAR
, EXEC_SQL_SNAP TEXT
, RESULT TEXT
, INPUT_PARAM VARCHAR
, TIME_START VARCHAR
, TIME_END VARCHAR
, TIME_ELAPSED VARCHAR
, EXEC_DATE VARCHAR
, REPEAT_NUM NUMERIC
, PARALLEL_NUM NUMERIC
, SESS_PLAN_NUM NUMERIC
, EXECUTE_BF_PLAN TEXT
, EXECUTE_AF_PLAN TEXT
)"""

SQLRESULT_INSERT_ROW="""INSERT INTO SQLRESULT VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""

SQLLIST_TRUNC_TAB="""TRUNCATE TABLE SQLLIST"""

SQLLIST_DROP_TAB="""DROP TABLE IF EXISTS SQLLIST"""

SQLLIST_CRE_TAB="""CREATE TABLE IF NOT EXISTS SQLLIST(
  SQL_ID VARCHAR
, DB_GB VARCHAR
, SQL_GB VARCHAR
, SQL_USAGE VARCHAR
, BIND_YN VARCHAR
, DBMS_TYPE VARCHAR
, SQL_TEXT TEXT
, SQL_DESC TEXT
, REPEAT_NUM NUMERIC
, PARALLEL_NUM NUMERIC
, SESS_PLAN_NUM NUMERIC
)"""

SQLLIST_INSERT_ROW="""INSERT INTO SQLLIST VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""

SQLLIST_CRE_PK="""ALTER TABLE sqllist ADD CONSTRAINT sqllist_pk PRIMARY KEY(SQL_ID)"""

SQLPARAM_TRUNC_TAB="""TRUNCATE TABLE SQLPARAM"""

SQLPARAM_DROP_TAB="""DROP TABLE IF EXISTS  SQLPARAM"""

SQLPARAM_CRE_TAB="""CREATE TABLE IF NOT EXISTS SQLPARAM(
  sql_id  VARCHAR
, param_num VARCHAR
, param01 VARCHAR
, param02 VARCHAR
, param03 VARCHAR
, param04 VARCHAR
, param05 VARCHAR
, param06 VARCHAR
, param07 VARCHAR
, param08 VARCHAR
, param09 VARCHAR
, param10 VARCHAR
)"""

SQLPARAM_INSERT_ROW="""INSERT INTO SQLPARAM VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""


SQL_COL_LIST="""SELECT column_name FROM information_schema.columns WHERE table_name = '%s' order by ordinal_position"""