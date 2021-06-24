import pandas as pd
from . import wiki_db
import calendar

import time
import utils.helper_functions as utils

def get_quarterly_report(PATH, quarter, year):

    #set-up
    prev_time = utils.fiscal_to_actual(quarter, year - 1) 
    curr_time = utils.fiscal_to_actual(quarter, year)

    prev_start = utils.format_time(1, prev_time["start_month"], prev_time["start_year"])
    prev_end = utils.format_time(1, prev_time["end_month"], prev_time["end_year"])
    curr_start = utils.format_time(1, curr_time["start_month"], curr_time["start_year"])
    curr_end = utils.format_time(1, curr_time["end_month"], curr_time["end_year"])
    
    #get data
    print("Beginning to generate gcWiki report")
    WIKI_DB = wiki_db()
    prev = WIKI_DB.get_all(prev_start, prev_end)
    curr = WIKI_DB.get_all(curr_start, curr_end)

    #close connection
    WIKI_DB.terminate()

    #merge data
    for i in range(len(prev)):
        prev[i].append(curr[i][1])

    #save it
    df = pd.DataFrame(prev)
    df.columns = ["field", "q" + str(quarter) + " " + str(year - 1), "q" + str(quarter) + " " + str(year) ]
    df.to_csv(PATH + "gcWiki_stats_q" + str(quarter) + "_" + str(year) + ".csv", index = False)
    print (PATH + "gcWiki_stats_q" + str(quarter) + "_" + str(year) + ".csv has been created.\n")
    

def get_monthly_report(PATH, month, year):

    #set-up
    print("Beginning to generate gcWiki report")
    month_name = calendar.month_name[month]
    prev_name = str(month_name) + " " + str(year - 1)
    curr_name = str(month_name) + " " + str(year) 
    
    prev_start = utils.format_time(1, month, year - 1)
    prev_end = utils.format_time(1, utils.monthly_increment(month, year - 1)["month"], utils.monthly_increment(month, year - 1)["year"])
    curr_start = utils.format_time(1, month, year)
    curr_end = utils.format_time(1, utils.monthly_increment(month, year)["month"], utils.monthly_increment(month, year)["year"])

    #get data
    WIKI_DB = wiki_db()
    prev = WIKI_DB.get_all(prev_start, prev_end)
    curr = WIKI_DB.get_all(curr_start, curr_end)

    #close connection
    WIKI_DB.terminate()

    #merge data
    for i in range(len(prev)):
        prev[i].append(curr[i][1])

    #save it
    df = pd.DataFrame(prev)
    df.columns = ["field", prev_name, curr_name]
    df.to_csv(PATH + "gcWiki_stats_" + month_name + "_"+ str(year) + ".csv", index = False)
    print (PATH + "gcWiki_stats_" + month_name + "_"+ str(year) + ".csv has been created.\n")
    
