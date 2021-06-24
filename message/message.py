#This file should be time-frame agnostic

import pandas as pd
import calendar
from pymongo import MongoClient
import datetime

from . import kube_connect

import utils.helper_functions as utils

class Message: 

    def __init__(self): 
        self.KUBE = kube_connect.db_connection()
        self.connect_to_database()
    
    def connect_to_database(self):
        while (not bool(self.KUBE.check_connection())):
            continue #waiting for db connection to be established

        self.myclient = MongoClient("mongodb://localhost:27017/")
        self.db = self.myclient["rocketchat"]

    def get_all(self, start, end):
        #print(start, end)
        stats_start = self.db["rocketchat_statistics"].find_one( { 'createdAt' : { '$lte' : start } } )
        stats_end = self.db["rocketchat_statistics"].find_one( { 'createdAt' : { '$lte' : end } } ) #Non func
                
        return [
            ["delta_users", self.db["users"].count_documents( { 'createdAt' : { '$gte' : start, '$lt' : end } } ) ],
            ["delta_channel_msgs", stats_end["totalChannelMessages"] - stats_start["totalChannelMessages"] ],
            ["delta_channel_rooms", stats_end["totalChannels"] - stats_start["totalChannels"] ],
            ["delta_direct_msgs", stats_end["totalDirectMessages"] - stats_start["totalDirectMessages"] ],
            ["delta_direct_rooms", stats_end["totalDirect"] - stats_start["totalDirect"] ],
            ["delta_private_group_msgs", stats_end["totalPrivateGroupMessages"] - stats_start["totalPrivateGroupMessages"] ],
            ["delta_private_group_rooms", stats_end["totalPrivateGroups"] - stats_start["totalPrivateGroups"] ],
            ["delta_uploads", self.db["rocketchat_uploads"].count_documents( { 'uploadedAt' : { '$gte' : start, '$lt' : end } } ) ],
            ["", ""],
            ["total_users", self.db["users"].count_documents( { 'createdAt' : { '$lt' : end } } ) ],
            ["total_messages", self.db["rocketchat_message"].count_documents( { 'ts' : { '$lt' : end } } ) ],
            ["total_uploads", self.db["rocketchat_uploads"].count_documents( { 'uploadedAt' : { '$lt' : end } } ) ],
            ["total_direct_msgs", stats_end["totalDirectMessages"] ],
            ["total_private_group_msgs", stats_end["totalPrivateGroupMessages"] ],
            ["total_channel_msgs", stats_end["totalChannelMessages"] ],
            ["total_direct_rooms", stats_end["totalDirect"] ],
            ["total_private_group_rooms", stats_end["totalPrivateGroups"] ],
            ["total_channel_rooms", stats_end["totalChannels"] ],
        ]
    
    def terminate(self):
        self.KUBE.terminate()
    
