import pandas as pd
from . import gccollab
import calendar

import utils.helper_functions as utils
       
def get_quarterly_report(PATH, quarter, year):

    #set up
    title = "q" + str(quarter) + "_" + str(year)

    column_names = {
        "previous": "q" + str(quarter) + " " + str(year - 1),
        "current": "q" + str(quarter) + " " + str(year) 
    }
    
    prev_values = utils.fiscal_to_actual(quarter, year - 1)
    curr_values = utils.fiscal_to_actual(quarter, year)
    
    prev_timeframe = {
        "start": utils.to_unixtime(1, prev_values["start_month"], prev_values["start_year"]),
        "end": utils.to_unixtime(1, prev_values["end_month"], prev_values["end_year"])
    }
    curr_timeframe = {
        "start": utils.to_unixtime(1, curr_values["start_month"], curr_values["start_year"]),
        "end": utils.to_unixtime(1, curr_values["end_month"], curr_values["end_year"])
    }
    
    make_report(PATH, prev_timeframe, curr_timeframe, column_names, title)


def get_monthly_report(PATH, month, year):

    #set up
    month_name = str(calendar.month_name[month])
    title = month_name + "_" + str(year)
    
    column_names = {
        "previous": month_name + " " + str(year - 1),
        "current": month_name + " " + str(year) 
    }
    
    prev_incremented = utils.monthly_increment(month, year - 1)
    curr_incremented = utils.monthly_increment(month, year)

    prev_timeframe = {
        "start": utils.to_unixtime(1, month, year - 1),
        "end": utils.to_unixtime(1, prev_incremented["month"], prev_incremented["year"])
    }
    curr_timeframe = {
        "start": utils.to_unixtime(1, month, year),
        "end": utils.to_unixtime(1, curr_incremented["month"], curr_incremented["year"])
    }
  
    make_report(PATH, prev_timeframe, curr_timeframe, column_names, title)


def make_report(PATH, prev_timeframe, curr_timeframe, column_names, title):
    
    GCCOLLAB_DB = gccollab.collab_db() 

    print("Beginning to generate gcCollab report")
    
    prev = GCCOLLAB_DB.get_all(prev_timeframe["start"], prev_timeframe["end"])
    curr = GCCOLLAB_DB.get_all(curr_timeframe["start"], curr_timeframe["end"])
    
    #merges them together
    for i in range(len(prev)):
        prev[i].append(curr[i][1])
    
    df = pd.DataFrame(prev)
    
    df.columns = ["field", column_names["previous"], column_names["current"]]
    df.to_csv(PATH + "gcCollab_stats_" + title + ".csv", index = False)
    print (PATH + "gcCollab_stats_" + title + ".csv has been created.\n")
    