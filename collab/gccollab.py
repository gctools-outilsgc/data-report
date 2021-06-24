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


class collab_db:
    '''
    Connects to the collab database to query it.
    '''

    def __init__(self):
        self.connect_to_database()

    def connect_to_database(self):
        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor()

    
    def get_all(self, start_unixtime, end_unixtime):
        '''
        Runs and returns the results of a series of queries, in the order they go into the monthly report 
        '''
        #print(start_unixtime, end_unixtime)
        return [
            #Platform Activity Values, in order
            ["delta_users", self.get_delta_users(start_unixtime, end_unixtime)],
            ["delta_groups", self.get_delta_groups(start_unixtime, end_unixtime)],
            ["delta_missions", self.get_delta_missions(start_unixtime, end_unixtime)],
            ["delta_discussions", self.get_delta_discussions(start_unixtime, end_unixtime)],
            ["delta_discussion_comments", self.get_delta_discussion_comments(start_unixtime, end_unixtime)],
            ["delta_blogs", self.get_delta_blogs(start_unixtime, end_unixtime)],
            ["delta_blog_comments", self.get_delta_blog_comments(start_unixtime, end_unixtime)],
            ["delta_wires", self.get_delta_wires(start_unixtime, end_unixtime)],
            ["delta_pages", self.get_delta_pages(start_unixtime, end_unixtime)],
            ["delta_page_comments", self.get_delta_page_comments(start_unixtime, end_unixtime)],
            ["delta_events", self.get_delta_events(start_unixtime, end_unixtime)],
            ["delta_etherpad", self.get_delta_etherpad(start_unixtime, end_unixtime)],
            ["delta_files", self.get_delta_files(start_unixtime, end_unixtime)],
            ["", ""],
            ["total_users", self.get_total_users(end_unixtime)],
            ["total_groups", self.get_total_groups(end_unixtime)],

            #Unused queries
            #["total_missions", self.get_total_missions(end_unixtime)],
            #["total_discussions", self.get_total_discussions(end_unixtime)],
            #["total_discussion_comments", self.get_total_discussion_comments(end_unixtime)],
            #["total_blogs", self.get_total_blogs(end_unixtime)],
            #["total_blog_comments", self.get_total_blog_comments(end_unixtime)],
            #["total_wires", self.get_total_wires(end_unixtime)],
            #["total_pages", self.get_total_pages(end_unixtime)],
            #["total_page_comments", self.get_total_page_comments(end_unixtime)],
            #["total_events", self.get_total_events(end_unixtime)],
            #["total_files", self.get_total_files(end_unixtime)],
            #["total_etherpad", self.get_total_etherpad(end_unixtime)]
        ]
    
    #All the functions for running the queries

    def get_delta_users(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) as delta_user FROM elggusers_entity ue
            JOIN elggentities ee ON ee.guid = ue.guid
            WHERE ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        users = pd.DataFrame(self.cursor.fetchall()) 
        users.columns = ["count"]
        return users["count"][0]

    def get_total_users(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) as user FROM elggusers_entity ue
            JOIN elggentities ee ON ee.guid = ue.guid
            WHERE ee.time_created < """+str(end_unixtime)+"""
            """
        )

        users = pd.DataFrame(self.cursor.fetchall()) 
        users.columns = ["count"]
        return users["count"][0]

    def get_delta_groups(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) as delta_user FROM elgggroups_entity ue
            JOIN elggentities ee ON ee.guid = ue.guid
            WHERE ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )
        groups = pd.DataFrame(self.cursor.fetchall()) 
        groups.columns = ["count"]
        return groups["count"][0]

    def get_total_groups(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) as user FROM elgggroups_entity ue
            JOIN elggentities ee ON ee.guid = ue.guid
            WHERE ee.time_created < """+str(end_unixtime)+"""
            """
        )

        groups = pd.DataFrame(self.cursor.fetchall()) 
        groups.columns = ["count"]
        return groups["count"][0]

    def get_delta_missions(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 52
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        missions = pd.DataFrame(self.cursor.fetchall()) 
        missions.columns = ["count"]
        return missions["count"][0]

    def get_total_missions(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
            WHERE ee.subtype = 52
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        missions = pd.DataFrame(self.cursor.fetchall()) 
        missions.columns = ["count"]
        return missions["count"][0]

    def get_delta_discussions(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 20
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        discussions = pd.DataFrame(self.cursor.fetchall()) 
        discussions.columns = ["count"]
        return discussions["count"][0]

    def get_total_discussions(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 20
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        discussions = pd.DataFrame(self.cursor.fetchall()) 
        discussions.columns = ["count"]
        return discussions["count"][0]

    def get_delta_discussion_comments(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 7) ee
            JOIN (
                SELECT * FROM elggentities
                WHERE subtype = 20
            ) parent_ee ON parent_ee.guid = ee.container_guid
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        discussion_comments = pd.DataFrame(self.cursor.fetchall()) 
        discussion_comments.columns = ["count"]
        return discussion_comments["count"][0]

    def get_total_discussion_comments(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 7) ee
            JOIN (
                SELECT * FROM elggentities
                WHERE subtype = 20
            ) parent_ee ON parent_ee.guid = ee.container_guid
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        discussion_comments = pd.DataFrame(self.cursor.fetchall()) 
        discussion_comments.columns = ["count"]
        return discussion_comments["count"][0]

    def get_delta_blogs(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 6
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        blogs = pd.DataFrame(self.cursor.fetchall()) 
        blogs.columns = ["count"]
        return blogs["count"][0]

    def get_total_blogs(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 6
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        blogs = pd.DataFrame(self.cursor.fetchall()) 
        blogs.columns = ["count"]
        return blogs["count"][0]

    def get_delta_blog_comments(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 4) ee
            JOIN (
                SELECT * FROM elggentities
                WHERE subtype = 6
            ) parent_ee ON parent_ee.guid = ee.container_guid
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        blog_comments = pd.DataFrame(self.cursor.fetchall()) 
        blog_comments.columns = ["count"]
        return blog_comments["count"][0]

    def get_total_blog_comments(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 4) ee
            JOIN (
                SELECT * FROM elggentities
                WHERE subtype = 6
            ) parent_ee ON parent_ee.guid = ee.container_guid
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        blog_comments = pd.DataFrame(self.cursor.fetchall()) 
        blog_comments.columns = ["count"]
        return blog_comments["count"][0]

    def get_delta_wires(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 8
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        wires = pd.DataFrame(self.cursor.fetchall()) 
        wires.columns = ["count"]
        return wires["count"][0]

    def get_total_wires(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 8
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        wires = pd.DataFrame(self.cursor.fetchall()) 
        wires.columns = ["count"]
        return wires["count"][0]

    def get_delta_pages(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 32
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        pages = pd.DataFrame(self.cursor.fetchall()) 
        pages.columns = ["count"]
        return pages["count"][0]

    def get_total_pages(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 32
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        pages = pd.DataFrame(self.cursor.fetchall()) 
        pages.columns = ["count"]
        return pages["count"][0]

    def get_delta_page_comments(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 4) ee
            JOIN (
                SELECT * FROM elggentities
                WHERE subtype = 32
            ) parent_ee ON parent_ee.guid = ee.container_guid
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        page_comments = pd.DataFrame(self.cursor.fetchall()) 
        page_comments.columns = ["count"]
        return page_comments["count"][0]

    def get_total_page_comments(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 4) ee
            JOIN (
                SELECT * FROM elggentities
                WHERE subtype = 32
            ) parent_ee ON parent_ee.guid = ee.container_guid
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        page_comments = pd.DataFrame(self.cursor.fetchall()) 
        page_comments.columns = ["count"]
        return page_comments["count"][0]

    def get_delta_events(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 18
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        events = pd.DataFrame(self.cursor.fetchall()) 
        events.columns = ["count"]
        return events["count"][0]

    def get_total_events(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 18
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        events = pd.DataFrame(self.cursor.fetchall()) 
        events.columns = ["count"]
        return events["count"][0]

    def get_delta_files(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 2
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        files = pd.DataFrame(self.cursor.fetchall()) 
        files.columns = ["count"]
        return files["count"][0]

    def get_total_files(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 2
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        files = pd.DataFrame(self.cursor.fetchall()) 
        files.columns = ["count"]
        return files["count"][0]

    def get_delta_etherpad(self, start_unixtime, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 59
            AND ee.time_created > """+str(start_unixtime)+""" AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        files = pd.DataFrame(self.cursor.fetchall()) 
        files.columns = ["count"]
        return files["count"][0]

    def get_total_etherpad(self, end_unixtime):
        self.cursor.execute(
            """
            SELECT COUNT(*) FROM elggentities ee
	        WHERE ee.subtype = 59
            AND ee.time_created < """+str(end_unixtime)+"""
            """
        )

        files = pd.DataFrame(self.cursor.fetchall()) 
        files.columns = ["count"]
        return files["count"][0]