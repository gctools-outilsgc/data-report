from .create_service import initialize_analyticsreporting, initialize_analyticsreporting_GA4
import pandas as pd
import time
import inflection

class gcga_v2:

    def __init__(self):
        self.analyticsGA4 = initialize_analyticsreporting_GA4()
        self.platformsGA4 =  {
            "gcCollab": "properties/383884917",
            "gcConnex": "properties/383883780",
            "gcPedia": "properties/383864912",
            "gcWiki": "properties/362244995"
        }
    
    def get_platform_metrics(self, platform="gccollab", startDate='2019-04-01', endDate='2019-06-26'):
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
                    {'expression': 'scrolledUsers', 'name': 'averagePageLoadTime'},  # not a GA4 metric, in fact it currently doesn't have any page timing metrics
                    {'expression': 'sessions', 'name': 'numsessions'},
                    {'expression': 'screenPageViewsPerSession', 'name': 'pageviewsPerSession'}
                ]
            }]
            }
        ).execute()
    
    def create_df(self, prev_response, current_response):
        """Parses and prints the Analytics Reporting API V4 response.

        Args:
        response: An Analytics Reporting API V4 response.
        """
        
        print(f"current: {current_response['reports']}") 
        data = []
        
        #gets all the data
        for x in range (7):
            name = prev_response["reports"][0]["metricHeaders"][x]["name"]
            
            try: #some tools (gcwiki) don't have data that far back (May 2018)
                prev_value = round (float(prev_response["reports"][0]["rows"][0]["metricValues"][x]["value"]), 2)
                if prev_value == int(prev_value):
                    prev_value = int(prev_value)
            except:
                prev_value = "N/A"

            try:
                current_value = round (float(current_response["reports"][0]["rows"][0]["metricValues"][x]["value"]), 2)
                if current_value == int(current_value):
                    current_value = int(current_value)
            except:
                current_value = "N/A"

            if name in ["numsessions", "pageviewsPerSession", "avgSessionDuration", "averagePageLoadTime", "bounce"]: #filters it, as we aren't using the rest in the reports 
                name = inflection.underscore(name) #converts it to snake_case
                data.append([name, prev_value, current_value])
        
        #orders the data
        order_names = ['numsessions', "pageviews_per_session", "avg_session_duration", "average_page_load_time", "bounce"]
        order = {key: i for i, key in enumerate(order_names)}
        data = sorted(data, key = lambda d: order[d[0]])
        
        df = pd.DataFrame(data)
        return df
    
