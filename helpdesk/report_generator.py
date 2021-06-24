import pandas as pd
import calendar
from datetime import date
import os

from helpdesk.helpdesk import Helpdesk as helpdesk
import utils.helper_functions as utils

def get_monthly_report(PATH, month, year):

    #set up
    prev = {
        "first_day": date(year - 1, month, 1),
        "last_day": date( year - 1, month, calendar.monthrange(year - 1, month)[1]) #Determines last day of a given month/year
    }

    curr = {
        "first_day": date(year, month, 1),
        "last_day": date( year, month, calendar.monthrange(year, month)[1]) #Determines last day of a given month/year

    }
        
    print ("Beginning to generate Helpdesk report")
    MONTH_NAME = calendar.month_name[month]
    print(MONTH_NAME)
    print(prev)
    print(curr)

    #Testing data left for future testing; to use instead of having to pull data
    if False: 
        curr = [[2, 59, 0, 104, 47, 2, 37], [5, 118, 2, 5, 39, 0, 37], [0, 0, 0, 0, 0, 0, 1], [1, 8, 0, 0, 12, 0, 2], [1, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 2]]
        prev = [[0, 0, 0, 0, 0, 2, 178], [0, 0, 0, 0, 0, 3, 175], [0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 25], [0, 0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 0, 0, 2]]
        curr_helpdesk_stats = pd.DataFrame(curr)
        prev_helpdesk_stats = pd.DataFrame(prev)

    #get dataframes

    #initialize
    prev_helpdesk = helpdesk()
    curr_helpdesk = helpdesk()
    
    #run
    print("Getting data (slow)")
    prev_helpdesk.calculate_counts(prev["first_day"], prev["last_day"])
    print("Halfway")
    curr_helpdesk.calculate_counts(curr["first_day"], curr["last_day"])
    print("Done getting data")

    #get values, put into df
    prev_helpdesk_stats = pd.DataFrame(prev_helpdesk.tag_counts)
    curr_helpdesk_stats = pd.DataFrame(curr_helpdesk.tag_counts)
        
    #merge and rearrange
    dfs = {} #Series of dfs for each tool. columns = years, rows = tag. We ignore other.
    tools = ["gcCollab", "gcConnex", "gcMessage", "gcPedia", "gcWiki"]
    for i in range(5):
        curr = MONTH_NAME + " " + str(year)
        prev = MONTH_NAME + " " + str(year - 1)
        dfs[tools[i]] = pd.DataFrame(data={prev: prev_helpdesk_stats.iloc[i, :], curr: curr_helpdesk_stats.iloc[i, :]} )
        dfs[tools[i]].index = ["Data", "Service Request", "Change Request", "Incident", "Assistance", "Other", "untagged"]

    #Write to csv
    for tool in tools: 
        if os.path.exists(PATH + tool + "_stats_" + calendar.month_name[int(month)] + "_" + str(year) + ".csv"):
            CSV_PATH = PATH + tool + "_stats_" + calendar.month_name[int(month)] + "_" + str(year) + ".csv"
            
            with open(CSV_PATH, 'a') as f:
                dfs[tool].columns = ["", ""]
                dfs[tool].to_csv(f, header= True, index=True)
                print ("Appended helpdesk data for " + tool + " to " +CSV_PATH )
                
        else:
            dfs[tool].index.name = "field"
            dfs[tool].to_csv(PATH + "freshdesk_" + tool + "_" + MONTH_NAME + "_" + str(year) + ".csv", index=True)
            print (PATH + "freshdesk_" + tool + "_" + MONTH_NAME + "_" + str(year) + ".csv has been created." )


def get_quarterly_report(PATH, quarter, year):
    prev_time = utils.fiscal_to_actual(quarter, year - 1)
    prev_time["end_month"], prev_time["end_year"] = utils.monthly_decrement(prev_time["end_month"], prev_time["end_year"])["month"], utils.monthly_decrement(prev_time["end_month"], prev_time["end_year"])["year"]
    
    curr_time = utils.fiscal_to_actual(quarter, year)
    curr_time["end_month"], curr_time["end_year"] = utils.monthly_decrement(curr_time["end_month"], curr_time["end_year"])["month"], utils.monthly_decrement(curr_time["end_month"], curr_time["end_year"])["year"]
    
    #helpdesk is set up to handle a month at a time, so this does it by the month
    prev_values = []
    curr_values = []
    for i in range(prev_time["start_month"], prev_time["end_month"] + 1, 1):
        prev = {
            "first_day": date(prev_time["start_year"], i, 1),
            "last_day": date( prev_time["end_year"], i, calendar.monthrange(prev_time["end_year"], i)[1]) #Determines last day of a given month/year
        }

        curr = {
            "first_day": date(curr_time["start_year"], i, 1),
            "last_day": date(curr_time["end_year"], i, calendar.monthrange(curr_time["end_year"], i)[1]) #Determines last day of a given month/year
        }
        prev_values.append(prev)
        curr_values.append(curr)
        
    print ("Beginning to generate Helpdesk report")

    #initialize
    prev_helpdesk = helpdesk()
    curr_helpdesk = helpdesk()
    
    #run
    print("Getting data (very slow)")
    for prev in prev_values:
        prev_helpdesk.calculate_counts(prev["first_day"], prev["last_day"])
    print("Halfway")
    for curr in curr_values:
        curr_helpdesk.calculate_counts(curr["first_day"], curr["last_day"])
    print("Done getting data")

    #get values, put into df
    prev_helpdesk_stats = pd.DataFrame(prev_helpdesk.tag_counts)
    curr_helpdesk_stats = pd.DataFrame(curr_helpdesk.tag_counts)
        
    #merge and rearrange
    dfs = {} #Series of dfs for each tool. columns = years, rows = tag. We ignore other.
    tools = ["gcCollab", "gcConnex", "gcMessage", "gcPedia", "gcWiki"]
    for i in range(5):
        curr = "q" + str(quarter) + " " + str(year)
        prev = "q" + str(quarter) + " " + str(year - 1)
        dfs[tools[i]] = pd.DataFrame(data={prev: prev_helpdesk_stats.iloc[i, :], curr: curr_helpdesk_stats.iloc[i, :]} )
        dfs[tools[i]].index = ["Data", "Service Request", "Change Request", "Incident", "Assistance", "Other", "untagged"]

    #Write to csv
    for tool in tools: 
        if os.path.exists(PATH + tool + "_stats_q" + str(quarter) + "_" + str(year) + ".csv"):
            CSV_PATH = PATH + tool + "_stats_q" + str(quarter) + "_" + str(year) + ".csv"
            with open(CSV_PATH, 'a') as f:
                dfs[tool].columns = ["", ""]
                dfs[tool].to_csv(f, header= True, index=True)
                print ("Appended helpdesk data for " + tool + " to " + CSV_PATH )
        else:
            dfs[tool].to_csv(PATH + "freshdesk_" + tool + "_q" + str(quarter) + "_" + str(year) + ".csv", index=True)
            print (PATH + "freshdesk_" + tool + "_q" + str(quarter) + "_" + str(year) + ".csv has been created." )



