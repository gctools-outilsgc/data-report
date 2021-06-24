import pandas as pd
import json

import mysql.connector
from mysql.connector import errorcode

import sqlalchemy as sq
import time
import sys

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

from sqlalchemy import text
from sqlalchemy.orm import aliased
from sqlalchemy import or_

from . import config
from . import kube_connect

class wiki_db:

    def __init__(self):
        self.DB = kube_connect.db_connection()
        self.connect_to_database()

    #Connects to the database
    def connect_to_database(self):
        while (not bool(self.DB.check_connection())):
            continue #waiting for db connection to be established

        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor()

    #uses series of functions to run queries
    def get_all(self, start_unixtime, end_unixtime):
        #print(start_unixtime, end_unixtime)
        return [
            ["delta_users", self.get_delta_users(start_unixtime, end_unixtime)],
            ["delta_articles", self.get_delta_articles(start_unixtime, end_unixtime)],
            ["delta_edits", self.get_delta_edits(start_unixtime, end_unixtime)],
            ["", ""],
            ["total_users", self.get_total_users(end_unixtime)],
            ["total_articles", self.get_total_articles(end_unixtime)],
            ["total_edits", self.get_total_edits(end_unixtime)],
        ]


    def get_total_users(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(CONVERT(user_registration USING utf8)) FROM user
            WHERE user_registration < """ + end_unixtime
        )

        users = pd.DataFrame(self.cursor.fetchall()) 
        users.columns = ["count"] #idk about this stuff
        return users["count"][0]

    def get_delta_users(self, start_unixtime, end_unixtime): #direct delta
        self.cursor.execute(
            """
            SELECT COUNT(CONVERT(user_registration USING utf8)) FROM user
            WHERE user_registration > """+ start_unixtime +""" AND user_registration < """+ end_unixtime
        )
        users = pd.DataFrame(self.cursor.fetchall()) 
        users.columns = ["count"]
        return users["count"][0]

    def get_total_articles(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM revision r
            WHERE rev_timestamp = (
                SELECT MIN(rev_timestamp) FROM revision r2
                WHERE r.rev_page = r2.rev_page
            ) AND rev_timestamp < """ + end_unixtime
        )
        articles = pd.DataFrame(self.cursor.fetchall())
        articles.columns = ["count"]
        return articles["count"][0]
    
    def get_delta_articles(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM revision r
            WHERE rev_timestamp = (
                SELECT MIN(rev_timestamp) FROM revision r2
                WHERE r.rev_page = r2.rev_page
            ) AND rev_timestamp > """ + start_unixtime + """ AND rev_timestamp < """ + end_unixtime
        )
        articles = pd.DataFrame(self.cursor.fetchall())
        articles.columns = ["count"]
        return articles["count"][0]

    def get_total_edits(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(CONVERT(rev_timestamp USING utf8)) FROM revision
            WHERE rev_timestamp < """ + end_unixtime
        )
        edits = pd.DataFrame(self.cursor.fetchall())
        edits.columns = ["count"]
        return edits["count"][0]
    
    def get_delta_edits(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(CONVERT(rev_timestamp USING utf8)) FROM revision
            WHERE rev_timestamp > """ + start_unixtime + """ AND rev_timestamp < """ + end_unixtime
        )
        edits = pd.DataFrame(self.cursor.fetchall())
        edits.columns = ["count"]
        return edits["count"][0]

    def terminate(self):
        self.DB.terminate()

  
   
    
    