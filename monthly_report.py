import calendar
import os
import sys
import datetime
from requests import head as requests_head

from connex import get_monthly_script as connex_get_script
from pedia import get_monthly_script as pedia_get_script
from collab import get_monthly_report as collab_get_report
from message import get_monthly_report as message_get_report
from google_analytics import get_monthly_report as google_analytics_get_report
from wiki import get_monthly_report as wiki_get_report
from helpdesk import get_monthly_report as helpdesk_get_report

"""
to get monthly report:
    python monthly_report.py {year} {month}
"""
if __name__ == "__main__": 

    #Verify args
    if len(sys.argv) != 3:
        raise Exception("Issue with arguments")
    elif int(sys.argv[2]) > 12 or int(sys.argv[2]) < 1:
        raise Exception("Invalid month")
    elif int(sys.argv[1]) > datetime.datetime.today().year + 1 or int(sys.argv[1]) < 2000:
        raise Exception("Invalid year")

    #rename args
    YEAR = int(sys.argv[1])
    MONTH = int(sys.argv[2])
    
    print("\nCreating reports for " + calendar.month_name[MONTH] + " " + str(YEAR) + "\n" )

    #create directory for storing data
    PATH = "reports/" + str(YEAR) + "-" + str(MONTH) + "/"
    if not os.path.exists(PATH[0:-1]):
        os.mkdir(PATH[0:-1])
        os.mkdir(PATH + "scripts")
    
    #Call functions
    if requests_head("https://www.google.com").status_code == 200:
        connex_get_script(PATH, MONTH, YEAR)
        pedia_get_script(PATH, MONTH, YEAR)
        print("\nQueries can now be sent\n")
        
        collab_get_report(PATH, MONTH, YEAR)
        message_get_report(PATH, MONTH, YEAR) #kubectl doesn't currently work for docker container
        wiki_get_report(PATH, MONTH, YEAR) #kubectl doesn't currently work for docker container
        google_analytics_get_report(PATH, MONTH, YEAR)
        helpdesk_get_report(PATH, MONTH, YEAR) #v slow
    else:
        print("Unable to connect. Make sure you're connected to the internet and try again")
        
