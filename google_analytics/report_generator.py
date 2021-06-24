from . import ga
import calendar
import pandas as pd
import os

import utils.helper_functions as utils


def get_monthly_report(PATH, MONTH, YEAR):
    
    #set up
    STR_MONTH = utils.string_month(MONTH)

    PREV_START = str(YEAR - 1) + "-" + STR_MONTH + "-01"
    PREV_END = str(YEAR - 1) + "-" + STR_MONTH + "-" + str(calendar.monthrange(int(YEAR), int(MONTH))[1]-1) #gets last day of month
    CURRENT_START = str(YEAR) + "-" + STR_MONTH + "-01" 
    CURRENT_END = str(YEAR) + "-" + STR_MONTH + "-" + str(calendar.monthrange(int(YEAR), int(MONTH))[1]-1) #gets last day of month

    print ("Beginning to generate GAstats reports")
    
    gcga = ga.gcga_v2()

    tools = ["gcCollab", "gcConnex", "gcPedia", "gcWiki"]

    for tool in tools:
        print ("Beginning to generate google analytics "+ tool + " report" )
        prev_report = gcga.get_platform_metrics(tool, startDate=PREV_START, endDate=PREV_END)
        current_report = gcga.get_platform_metrics(tool, startDate=CURRENT_START, endDate=CURRENT_END)
        
        df = gcga.create_df(prev_report, current_report)
        df.columns = ["field", calendar.month_name[int(MONTH)] + " " + str(YEAR - 1), calendar.month_name[int(MONTH)] + " " + str(YEAR)]
        
        df.set_index = "field"
        if os.path.exists(PATH + tool + "_stats_" + calendar.month_name[int(MONTH)] + "_" + str(YEAR) + ".csv"):
            CSV_PATH = PATH + tool + "_stats_" + calendar.month_name[int(MONTH)] + "_" + str(YEAR) + ".csv"
            
            with open(CSV_PATH, 'a') as f:
                df.to_csv(f, header= False, index=False)
                print ("Appended google analytics data for " + tool + " to " +CSV_PATH + "\n")
                
        else:
            df.to_csv(PATH + "google_analytics_" + tool + "_" + calendar.month_name[int(MONTH)] + "_" + str(YEAR) + ".csv", index=False)
            print (PATH + "google_analytics_" + tool + "_" + calendar.month_name[int(MONTH)] + "_" + str(YEAR) + ".csv has been created.\n" )
    


def get_quarterly_report(PATH, QUARTER, YEAR):

    #Set up
    curr = utils.fiscal_to_actual(QUARTER, YEAR) # {"start_month": initial_month, "end_month": final_month, "year": year} 
    curr["end_month"] = utils.monthly_decrement(curr["end_month"], curr["end_year"])["month"]
    curr["end_year"] = utils.monthly_decrement(curr["end_month"], curr["end_year"])["year"]

    prev = utils.fiscal_to_actual(QUARTER, YEAR - 1) # {"start_month": initial_month, "end_month": final_month, "year": year} 
    prev["end_month"] = utils.monthly_decrement(prev["end_month"], prev["end_year"])["month"]
    prev["end_year"] = utils.monthly_decrement(prev["end_month"], prev["end_year"])["year"]

    PREV_START = str(prev["start_year"]) + "-" + utils.string_month(prev["start_month"]) + "-01"
    PREV_END = str(prev["end_year"]) + "-" + utils.string_month(prev["end_month"]) + "-" + str(calendar.monthrange(prev["end_year"], prev["end_month"])[1]) #gets last day of month
    CURRENT_START = str(curr["start_year"]) + "-" + utils.string_month(curr["start_month"]) + "-01" 
    CURRENT_END = str(curr["end_year"]) + "-" + utils.string_month(curr["end_month"]) + "-" + str(calendar.monthrange(curr["end_year"], curr["end_month"])[1]) #gets last day of month

    print ("Beginning to generate GAstats reports")
    gcga = ga.gcga_v2()
    tools = ["gcCollab", "gcConnex", "gcPedia", "gcWiki"]

    for tool in tools:
        print ("Beginning to generate google analytics "+ tool + " report" )
        prev_report = gcga.get_platform_metrics(tool, startDate=PREV_START, endDate=PREV_END)
        current_report = gcga.get_platform_metrics(tool, startDate=CURRENT_START, endDate=CURRENT_END)
        
        df = gcga.create_df(prev_report, current_report)
        df.columns = ["field", "Q" + str(QUARTER) + " " + str(YEAR - 1), "Q" + str(QUARTER) + " " + str(YEAR)]
        
        df.set_index = "field"
        if os.path.exists(PATH + tool + "_stats_q" + str(QUARTER) + "_" + str(YEAR) + ".csv"):
            CSV_PATH = PATH + tool + "_stats_q" + str(QUARTER) + "_" + str(YEAR) + ".csv"
            
            with open(CSV_PATH, 'a') as f:
                df.to_csv(f, header= False, index=False)
                print ("Appended google analytics data for " + tool + " to " +CSV_PATH + "\n")
                
        else:
            df.to_csv(PATH + "google_analytics_" + tool + "_q" + str(QUARTER) + "_" + str(YEAR) + ".csv", index=False)
            print (PATH + "google_analytics_" + tool + "_q" + str(QUARTER) + "_" + str(YEAR) + ".csv has been created." + "\n")
