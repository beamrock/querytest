# -*- coding: utf-8 -*-
##########################################################################################
### Declare Import Libraries
## Concerned with Default Setting
#import datetime
from datetime import datetime
import time
import pandas as pd

# Time Variable
now      = datetime.now()
nowDate  = now.strftime('%Y%m%d') 
nowFullTime  = now.strftime('%Y%m%d%H%M%S') 

def date_range(start,end):
    start=datetime.strptime(start, "%Y%m%d")
    end=datetime.strptime(end, "%Y%m%d")
    dates = [date.strftime("%Y%m%d") for date in pd.date_range(start, periods=(end-start).days+1)]
    #print(type(dates))
    return dates

def get_this_time():
    this_time = datetime.fromtimestamp(time.time())
    this_time_str = this_time.strftime('%Y-%m-%d %H:%M:%S')
    #print ("This Time: ",this_time_str)
    return this_time, this_time_str

def get_elapsed_time(time_start, time_end):
    elapsed = time_end - time_start
    elapsed_sec = float(elapsed.seconds)
    elapsed_str = str(elapsed).split('.')
    elapsed_time_str = elapsed_str[0]
    #print ("elapsed: ",elapsed_time)
    #print(type(elapsed_sec), ' / ', type(elapsed_time_str))
    #print(elapsed_sec, elapsed_time_str)
    return elapsed_sec, elapsed_time_str




