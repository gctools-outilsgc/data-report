import pandas as pd
import calendar
from pymongo import MongoClient
import datetime

from . import message
from . import kube_connect
import utils.helper_functions as utils

def get_monthly_report(PATH, month, year):

    #set up
    month_name = calendar.month_name[month]
    prev_name = month_name + " " + str(year - 1)
    curr_name = month_name + " " + str(year)
    title = month_name + "_"+ str(year)
    
    prev = {
        "start": datetime.datetime(year - 1, month, 1, 0, 0, 0, 0),
        "end": datetime.datetime(utils.monthly_increment(month, year - 1)["year"], utils.monthly_increment(month, year - 1)["month"], 1, 0, 0, 0, 0)
    }

    curr = {
        "start": datetime.datetime(year, month, 1, 0, 0, 0, 0),
        "end": datetime.datetime(utils.monthly_increment(month, year)["year"], utils.monthly_increment(month, year)["month"], 1, 0, 0, 0, 0)
    }
    
    #get data
    print ("Beginning to generate gcMessage report")
    try:
        DB = message.Message()

        prev = DB.get_all(prev["start"], prev["end"])
        current = DB.get_all(curr["start"], curr["end"])
    
    finally: #close connection
        DB.terminate()

    #merge data
    for i in range(len(prev)):
        prev[i].append(current[i][1])
    
    #store data
    df = pd.DataFrame(prev)
    df.columns = ["field", prev_name, curr_name]
    df.to_csv(PATH + "gcMessage_stats_" + title + ".csv", index = False)
    print (PATH + "gcMessage_stats_" + title + ".csv has been created.\n")


def get_quarterly_report(PATH, quarter, year):

    #set up
    prev_name = "q" + str(quarter) + " " + str(year - 1)
    curr_name = "q" + str(quarter) + " " + str(year)
    title = "q" + str(quarter) + "_"+ str(year)
    
    prev_quarter = utils.fiscal_to_actual(quarter, year - 1) 
    curr_quarter = utils.fiscal_to_actual(quarter, year)
    prev = {
        "start": datetime.datetime(prev_quarter["start_year"], prev_quarter["start_month"], 1, 0, 0, 0, 0),
        "end": datetime.datetime(prev_quarter["end_year"], prev_quarter["end_month"], 1, 0, 0, 0, 0)
    }

    curr = {
        "start": datetime.datetime(curr_quarter["start_year"], curr_quarter["start_month"], 1, 0, 0, 0, 0),
        "end": datetime.datetime(curr_quarter["end_year"], curr_quarter["end_month"], 1, 0, 0, 0, 0)
    }

    #get data
    print ("Beginning to generate gcMessage report")
    try:
        DB = message.Message()

        prev = DB.get_all(prev["start"], prev["end"])
        current = DB.get_all(curr["start"], curr["end"])
    
    finally: #close connection
        DB.terminate()

    #merge data
    for i in range(len(prev)):
        prev[i].append(current[i][1])
    
    #store data
    df = pd.DataFrame(prev)
    df.columns = ["field", prev_name, curr_name]
    df.to_csv(PATH + "gcMessage_stats_" + title + ".csv", index = False)
    print (PATH + "gcMessage_stats_" + title + ".csv has been created.\n")