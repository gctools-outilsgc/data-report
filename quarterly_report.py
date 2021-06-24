import calendar
import os
import sys
import datetime
from requests import head as requests_head

from connex import get_quarterly_script as connex_get_script
from pedia import get_quarterly_script as pedia_get_script
from collab import get_quarterly_report as collab_get_report
from message import get_quarterly_report as message_get_report
from google_analytics import get_quarterly_report as google_analytics_get_report
from wiki import get_quarterly_report as wiki_get_report
from helpdesk import get_quarterly_report as helpdesk_get_report

"""
to get quarterly report:
    python quarterly_report.py {year} {quarter}
"""
if __name__ == "__main__": 

    #Verify args
    if len(sys.argv) != 3:
        raise Exception("Issue with arguments")
    elif int(sys.argv[2]) > 4 or int(sys.argv[2]) < 1:
        raise Exception("Invalid quarter")
    elif int(sys.argv[1]) > datetime.datetime.today().year + 1 or int(sys.argv[1]) < 2000:
        raise Exception("Invalid year")

    #rename args
    YEAR = int(sys.argv[1])
    QUARTER = int(sys.argv[2])
    
    print("\nCreating reports for quarter number " + str(QUARTER) + ", " + str(YEAR) + "\n" )

    #create directory for storing data
    PATH = "reports/" + str(YEAR) + "-q" + str(QUARTER) + "/"
    if not os.path.exists(PATH[0:-1]):
        os.mkdir(PATH[0:-1])
        os.mkdir(PATH + "scripts")
    
    #Call functions
    if requests_head("http://www.google.com").status_code == 200:
        connex_get_script(PATH, QUARTER, YEAR)
        pedia_get_script(PATH, QUARTER, YEAR)
        print("\nQueries can now be sent\n")
        
        collab_get_report(PATH, QUARTER, YEAR)
        message_get_report(PATH, QUARTER, YEAR) #kubectl doesn't currently work for docker container
        wiki_get_report(PATH, QUARTER, YEAR) #kubectl doesn't currently work for docker container
        google_analytics_get_report(PATH, QUARTER, YEAR)
        helpdesk_get_report(PATH, QUARTER, YEAR) #v slow
    else:
        print("Unable to connect. Make sure you're connected to the internet and try again")



