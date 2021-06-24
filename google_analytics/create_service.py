from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import os

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
dirpath = os.getcwd() #So it runs in docker container and locally
KEY_FILE_LOCATION = dirpath + '/google_analytics' + '/client_secrets.json'

def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
    An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics