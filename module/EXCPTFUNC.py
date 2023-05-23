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


# https://kb.objectrocket.com/postgresql/python-error-handling-with-the-psycopg2-postgresql-adapter-645
# define a function that handles and parses psycopg2 exceptions
def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")
    
    return err, line_num, traceback, err_type, err.diag, err.pgerror, err.pgcode
