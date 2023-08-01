from .create_service import initialize_analyticsreporting
import pandas as pd
import time
import inflection

class gcga_v2:

    def __init__(self):
        self.analytics = initialize_analyticsreporting()
        self.platforms =  {
            "gcCollab": "ga:127642570",
            "gcConnex": "ga:55943097",
            "gcPedia": "ga:39673253",
            "gcWiki": "ga:177027196"
        }
        self.platformsGA4 =  {
            "gcCollab": "properties/383884917",
            "gcConnex": "properties/383883780",
            "gcPedia": "properties/383864912",
            "gcWiki": "properties/362244995"
        }
    
    def get_platform_metrics(self, platform="gccollab", startDate='2019-04-01', endDate='2019-06-26'):
        #print(startDate, endDate)
        if (startDate >= '2023-07-01'):
            return self.analyticsGA4.properties().batchRunReports(
                property = self.platformsGA4[platform],
                body={
                'requests': [
                {
                    'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                    'metrics': [
                        {'expression': 'screenPageViews', 'name': 'pageviews'},
                        {'expression': 'totalUsers', 'name': 'users'},  # or activeUsers
                        {'expression': 'averageSessionDuration', 'name': 'avgSessionDuration'},
                        {'expression': 'bounceRate', 'name': 'bounce'}, # (Sessions Minus Engaged sessions) divided by Sessions), where an engaged session lasted “longer than 10 seconds, or had a conversion event, or had 2 or more screen views.”
                        {'expression': '', 'name': 'averagePageLoadTime'},  # not a GA4 metric
                        {'expression': 'sessions', 'name': 'numsessions'},
                        {'expression': 'screenPageViewsPerSession', 'name': 'pageviewsPerSession'}
                    ]
                }]
                }
            ).execute()
            
        return self.analytics.reports().batchGet(
            body={
            'reportRequests': [
            {
                'viewId': self.platforms[platform],
                'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                'metrics': [
                    {'expression': 'ga:pageviews'},
                    {'expression': 'ga:users'},
                    {'expression': 'ga:avgSessionDuration'},
                    {'expression': 'ga:bounceRate'},
                    {'expression': 'ga:avgPageLoadTime'},
                    {'expression': 'ga:sessions'},
                    {'expression': 'ga:pageviewsPerSession'}
                ]
            }]
            }
        ).execute()
    
    def create_df(self, prev_response, current_response):
        """Parses and prints the Analytics Reporting API V4 response.

        Args:
        response: An Analytics Reporting API V4 response.
        """
        
        data = []
        
        #gets all the data
        for x in range (7):
            name = prev_response["reports"][0]["columnHeader"]["metricHeader"]["metricHeaderEntries"][x]["name"]
            
            try: #some tools (gcwiki) don't have data that far back (May 2018)
                prev_value = round (float(prev_response["reports"][0]["data"]["rows"][0]["metrics"][0]["values"][x]), 2)
                if prev_value == int(prev_value):
                    prev_value = int(prev_value)
            except:
                prev_value = "N/A"

            try:
                #current_value = round (float(current_response["reports"][0]["data"]["rows"][0]["metrics"][0]["values"][x]), 2)
                current_value = round (float(current_response["reports"][0]["rows"][0]["metricValues"][x]["value"]), 2)            # either this will need to be handled based on response type or in about a year it can simply completely replace the old response handling
                if current_value == int(current_value):
                    current_value = int(current_value)
            except:
                current_value = "N/A"

            if name in ["ga:sessions", "ga:pageviewsPerSession", "ga:avgSessionDuration", "ga:avgPageLoadTime", "ga:bounceRate"]: #filters it, as we aren't using the rest in the reports 
                name = name[3:]
                name = inflection.underscore(name) #converts it to snake_case
                data.append([name, prev_value, current_value])
        
        #orders the data
        order_names = ['sessions', "pageviews_per_session", "avg_session_duration", "avg_page_load_time", "bounce_rate"]
        order = {key: i for i, key in enumerate(order_names)}
        data = sorted(data, key = lambda d: order[d[0]])
        
        df = pd.DataFrame(data)
        return df
    
            


