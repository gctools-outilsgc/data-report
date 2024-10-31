import requests
import json
from helpdesk.config import config
import calendar
from datetime import date
import pandas as pd
import time
#currently doesn't handle error of exceeding API calls, but that's unlikely to happen

#encoding meanings
# %22   "
# %27   '
# %20   space

#Class uses a matrix to store the number of each tag type for each tool 
class Helpdesk:

    #dict, mapping tool to num
    tools = {
        "2100000290": 0, #gcCollab
        "2100000289": 1, #gcConnex
        "2100000661": 2, #gcMessage
        "2100000298": 3, #gcPedia
        "2100000516": 4, #gcWiki
        "2100000896": 7, #GCaccount
        "2100000673": 99, #gcConsult - get the proper one from the application field
        "2100000869": 6, #gcExchange
        "2100000979": 8, #rapid testing
        "21000000025": 9, # something else?
        "None": 5 #other, no tool assigned
    }

    #dict, mapping tag to num
    tag = {
        "Data": 0,
        "Service Request": 1,
        "Change Request": 2,
        "Incident": 3,
        "Assistance": 4,
        "Other": 5,
        "untagged": 6
    }

    def __init__(self):
        self.AUTH = (config['apikey'], 'X')
        self.URL = "https://" + config['domain'] + ".freshdesk.com/api/v2/search/tickets?query="
        self.tag_counts =  [[0] * 7 for i in range(9)] #creates 7 * 6 matrix
        
    
    def calculate_counts(self, start, end):
        '''
        Recursive function. Pagination wasn't working and so a work-around was required. Hence, recursion. Also, Jess' pride and joy
        '''
        #print(start, end)
        x = self.get_data(start, end)
        #print("Doing:", start, end)
        if x['total'] < 300: #300 is cap, set by freshdesk
            #other = [] #In case you want to see what the other tags are

            for i in range(1, x['total'] // 30 + 2):
                #pull corresponding page
                tickets = self.get_data(start, end, i)['results'] #Find better append
                
                #analyze it
                for ticket in tickets:
                    # determine product type corresponding number
                    prod_id = self.tools[str(ticket.get('product_id'))]
                    
                    if prod_id == 99:
                        print(str(ticket.get('cf_application')))
                        prod_id = 0
                    
                    # determine tag type
                    tag_type = 6
                    tags = ', '.join(ticket.get('tags')).lower()
                    if 'data' in tags:
                        tag_type = 0
                    elif 'service' in tags:
                        tag_type = 1 
                    elif 'change' in tags:
                        tag_type = 2
                    elif 'incident' in tags:
                        tag_type = 3
                    elif 'assistance' in tags:
                        tag_type = 4
                    elif len(tags) != 0:
                        tag_type = 5
                        #other.append(tags)
                    
                    #increment
                    self.tag_counts[prod_id][tag_type] += 1

        else:
            time.sleep(5) #Can't ping it too fast
            self.calculate_counts(start, date(start.year, start.month, (start.day + end.day) // 2) )
            time.sleep(5)
            self.calculate_counts(date(start.year, start.month, (start.day + end.day) // 2 + 1), end)

    
    def form_params(self, start_date, end_date, page=1):
        '''
        Function to form the query that's sent to the API

        @param start_date and end_date should both be date objects
        '''
        query1 = "created_at:>" + "%27" + str(start_date) + "%27" #greater than or equal to
        query2 = "created_at:<" + "%27" + str(end_date) + "%27" #less than or equal to. There is no strictly less than
        query3 = "&page=" + str(page)

        query = "%22" + query1 + "%20AND%20" + query2 + "%22" + query3 
        
        return query

    
    def get_data(self, start_date, end_date, page=1):
        '''
        Function for querying the API and handling the results
        '''
        
        params = self.form_params(start_date, end_date, page) #Format query
        r = requests.get(self.URL + params, auth = self.AUTH)
        
        if r.status_code == 200:
            ans = json.loads(r.content)
            return ans

        else: #404 = url issue
            print("Failed to read tickets, errors are displayed below,")
            print("Status Code : " + str(r.status_code))

            response = json.loads(r.content)
            print(response["errors"])

            print("x-request-id : " + r.headers['x-request-id'])
            
