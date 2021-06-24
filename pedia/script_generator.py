
import datetime
import calendar

import utils.helper_functions as utils
from . import queries

def get_monthly_script(PATH, month, year):

    PATH = PATH + "scripts/"

    #Time formatting
    current_start = utils.format_time(1, month, year)
    current_end = utils.format_time(1, utils.monthly_increment(month, year)["month"], utils.monthly_increment(month, year)["year"])

    prev_start = utils.format_time(1, month, year - 1)
    prev_end = utils.format_time(1, utils.monthly_increment(month, year - 1)["month"], utils.monthly_increment(month, year - 1)["year"])

    print("Beginning to generate gcPedia queries to send to Ilia.")
    prev = create_query_strings(prev_start, prev_end, year - 1)
    curr = create_query_strings(current_start, current_end, year)
   
    count = 1 #Keeps queries in order in directory

    for query in prev:
        with open(PATH + str(count) + "_gcPedia_" + query["name"] + "_" + str(year - 1) + "_" + str(month) + ".sql", "w" )  as f:
            f.write(query["query"])
            
        print(PATH + str(count) + "_gcPedia_" + query["name"] + "_" + str(year - 1) + "_" + str(month) + ".sql has been created.")
        count += 1
    
    for query in curr:
        with open(PATH + str(count) + "_gcPedia_" + query["name"] + "_" + str(year) + "_" + str(month) + ".sql", "w" )  as f:
            f.write(query["query"])
        print(PATH + str(count) + "_gcPedia_" + query["name"] + "_" + str(year) + "_" + str(month) + ".sql has been created.")
        count += 1

def create_query_strings(start_date, end_date, year):
    all_queries = [
        {"name": "delta_users","query": queries.delta_users.format(END_TIME = end_date, START_TIME = start_date, YEAR = year)},
        {"name": "delta_articles","query": queries.delta_articles.format(END_TIME = end_date, START_TIME = start_date, YEAR = year)},
        {"name": "delta_edits","query": queries.delta_edits.format(END_TIME = end_date, START_TIME = start_date, YEAR = year) },
        {"name": "total_users","query": queries.total_users.format(END_TIME = end_date, YEAR = year)},
        {"name": "total_articles","query": queries.total_articles.format(END_TIME = end_date, YEAR = year)},
        {"name": "total_edits","query": queries.total_edits.format(END_TIME = end_date, YEAR = year)}
    ]
    return all_queries

def get_quarterly_script(PATH, quarter, year):

    PATH = PATH + "scripts/"
    prev_title = "_q" + str(quarter) + "_" + str(year - 1)
    curr_title = "_q" + str(quarter) + "_" + str(year)
    #Time formatting
    curr = utils.fiscal_to_actual(quarter, year) #{"start_month": initial_month, "end_month": final_month, "year": year}
    current_start = utils.format_time(1, curr["start_month"], curr["start_year"])
    current_end = utils.format_time(1, curr["end_month"], curr["end_year"])

    prev = utils.fiscal_to_actual(quarter, year - 1) #{"start_month": initial_month, "end_month": final_month, "year": year}
    prev_start = utils.format_time(1, prev["start_month"], prev["start_year"])
    prev_end = utils.format_time(1, prev["end_month"], prev["end_year"])

    print("Beginning to generate gcPedia queries to send to Ilia.")
    prev = create_query_strings(prev_start, prev_end, prev["start_year"])
    curr = create_query_strings(current_start, current_end, curr["start_year"])
    
    count = 1

    for query in prev:
        with open(PATH + str(count) + "_gcPedia_" + query["name"] + prev_title + ".sql", "w" )  as f:
            f.write(query["query"])
            
        print(PATH + str(count) + "_gcPedia_" + query["name"] + prev_title + ".sql has been created.")
        count += 1
    
    for query in curr:
        with open(PATH + str(count) + "_gcPedia_" + query["name"] + curr_title + ".sql", "w" )  as f:
            f.write(query["query"])
        print(PATH + str(count) + "_gcPedia_" + query["name"] + curr_title + ".sql has been created.")
        count += 1
    

