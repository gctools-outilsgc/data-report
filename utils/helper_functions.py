import time
import datetime


def to_unixtime(day, month, year):
    """
    converts a given day, month, year to its unixtime
    """
    s = "1/" + str(day) + "/" + str(month) + "/" + str(year)
    return int(time.mktime(datetime.datetime.strptime(s, "%H/%d/%m/%Y").timetuple()))


def monthly_increment(month, year):
    """
    Increments the given month, year by one month.
    """
    if month < 12:
        return {"month" : month + 1, "year" : year}
    else:
        return {"month" : 1, "year" : year + 1}


def monthly_decrement(month, year):
    """
    Decrements the given month, year by one month.
    """ 
    if month == 1 :
        return {"month" : 12, "year" : year - 1}
    else:
        return {"month" : month - 1, "year" : year}


def string_month(month):
    """
    Returns formatted month string, that's always a 2 character string
    """
    str_month = str(month)
    if month < 10:
        str_month = "0" + str_month
    return str_month


def format_time(day, month, year):
    """
    Returns string with date format YYYYMMDDHHMMSS
    """
    year = str(year)
    month = str(month)
    day = str(day)

    if len(month) == 1:
        month = "0" + month
    if len(day) == 1:
        day = "0" + day
        
    return year + month + day + "000000" 


def fiscal_to_actual(quarter_num, fiscal_year):
    '''
    Used for making quarterly reports, to convert from fiscal info into actual month/years
    '''
    initial_month = (1 + quarter_num * 3) % 12
    
    start_year = fiscal_year

    final_month = (initial_month + 3) % 12

    end_year = fiscal_year

    if quarter_num > 2:
        end_year = fiscal_year + 1

    if quarter_num == 4:
        start_year = fiscal_year + 1

    tup = {"start_month": initial_month, "end_month": final_month, "start_year": start_year, "end_year": end_year} #End month is actually one more, as it's only used with to_unixtime where we do start < time < end 
    #print (tup)
    return tup

    

