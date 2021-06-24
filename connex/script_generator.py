
import datetime
import calendar

import utils.helper_functions as utils
from . import queries

def get_monthly_script(PATH, month, year):
    
    #set up
    PATH = PATH + "scripts/"

    prev_incremented = utils.monthly_increment(month, year - 1)
    curr_incremented = utils.monthly_increment(month, year)

    prev_timeframe = {
        "start": { "day": 1, "month" : month, "year": year - 1},
        "end": { "day": 1, "month": prev_incremented["month"], "year": prev_incremented["year"]}
    }

    curr_timeframe = {
        "start": { "day": 1, "month": month, "year": year},
        "end": { "day": 1, "month": curr_incremented["month"], "year": curr_incremented["year"]}
    }

    #Make strings of queries
    print("Beginning to generate gcConnex queries to send to Ilia.")
    prev = create_strings_monthly(prev_timeframe["start"], prev_timeframe["end"])
    curr = create_strings_monthly(curr_timeframe["start"], curr_timeframe["end"])
    
    #Store
    with open(PATH + "gcConnex_query_" + str(prev_timeframe["start"]["year"]) + "_" + str(prev_timeframe["start"]["month"]) + ".sql", "w" )  as f:
        f.write(prev)
    print (PATH + "gcConnex_query_" + str(prev_timeframe["start"]["year"]) + "_" + str(prev_timeframe["start"]["month"]) + ".sql has been created.\n")

    with open(PATH + "gcConnex_query_" + str(curr_timeframe["start"]["year"]) + "_" + str(curr_timeframe["start"]["month"]) + ".sql", "w" )  as f:
        f.write(curr)
    print (PATH + "gcConnex_query_" + str(curr_timeframe["start"]["year"]) + "_" + str(curr_timeframe["start"]["month"]) + ".sql has been created.")
    
#Creates strings
def create_strings_monthly(start_date, end_date):
    #print(start_date, end_date)
    DISPLAY_NAME = "{} {}".format(str(calendar.month_name[start_date["month"]]), str( start_date["year"] ))
    FUNCTIONAL_NAME = "{}_{}_".format(str(calendar.month_name[start_date["month"]]) , str( start_date["year"] ))

    INITIAL_TIME = utils.to_unixtime(start_date["day"], start_date["month"], start_date["year"])
    END_TIME = utils.to_unixtime(end_date["day"], end_date["month"], end_date["year"])

    return queries.query.format(DISPLAY_NAME = DISPLAY_NAME, FUNCTIONAL_NAME = FUNCTIONAL_NAME, INITIAL_TIME = INITIAL_TIME, END_TIME = END_TIME)


def get_quarterly_script(PATH, quarter, year):
    PATH = PATH + "scripts/"
    
    prev_values = utils.fiscal_to_actual(quarter, year - 1)
    curr_values = utils.fiscal_to_actual(quarter, year)

    prev_timeframe = {
        "start": [ 1, prev_values["start_month"], prev_values["start_year"] ],
        "end": [ 1, prev_values["end_month"], prev_values["end_year"] ]
    }
    curr_timeframe = {
        "start": [ 1, curr_values["start_month"], curr_values["start_year"] ],
        "end": [ 1, curr_values["end_month"], curr_values["end_year"] ]
    }

    print("Beginning to generate gcConnex queries to send to Ilia.")
    prev = create_strings_quarterly(prev_timeframe["start"], prev_timeframe["end"])
    curr = create_strings_quarterly(curr_timeframe["start"], curr_timeframe["end"])
    
    with open(PATH + "gcConnex_query_" + str(year) + "_q" + str(quarter) + ".sql", "w" )  as f:
        f.write(curr)
    
    with open(PATH + "gcConnex_query_" + str(year - 1) + "_q" + str(quarter) + ".sql", "w" )  as f:
        f.write(prev)

    print (PATH + "gcConnex_query_" + str(year) + "_q" + str(quarter) + ".sql has been created.")
    print (PATH + "gcConnex_query_" + str(year - 1) + "_q" + str(quarter) + ".sql has been created.\n")


#Creates strings
def create_strings_quarterly(start_date, end_date):
    #print(start_date, end_date)
    tmp_num_display = ""
    tmp_num_functional = ""
    tmp_year = ""

    if start_date[1] == 1: #not a great system
        tmp_num_display = "quarter #4"
        tmp_num_functional = "q4"
        tmp_year = (start_date[2]) - 1
        
    else: 
        tmp_num_display = "quarter #{}".format(str(int(((start_date[1] - 1) / 3) % 12)))
        tmp_num_functional = "q{}".format(str(int(((start_date[1] - 1) / 3) % 12)))
        tmp_year = str( start_date[2] )
    
    DISPLAY_NAME = "{} {}".format(tmp_num_display, tmp_year)
    FUNCTIONAL_NAME = "{}_{}_".format(tmp_num_functional, tmp_year)

    INITIAL_TIME = utils.to_unixtime(start_date[0], start_date[1], start_date[2])
    END_TIME = utils.to_unixtime(end_date[0], end_date[1], end_date[2])

    return queries.query.format(DISPLAY_NAME = DISPLAY_NAME, FUNCTIONAL_NAME = FUNCTIONAL_NAME, INITIAL_TIME = INITIAL_TIME, END_TIME = END_TIME)





