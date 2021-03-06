"""The Pirate Bay Module."""

import logging
import sys
import pprint
import datetime
import threading
import Queue
import requests
import time
import bs4
import copy
import re
import rospy

from hs_utils import ros_node 
from hs_utils.mongo_handler import MongoAccess
from bs4 import BeautifulSoup
from collections import Counter
from torrench.utilities.Config import Config
from limetorrents_crawler import LimeTorrentsCrawler

class TorrentsController:#(LimeTorrentsCrawler):

    def __init__(self, **kwargs):
        """Initialisations."""
        try:
            self.run_crawler    = threading.Condition()
            self.run_parser     = threading.Condition()
            self.run_post= threading.Condition()
            
            self.db_handler         = None
            self.title              = None
            self.pages              = None
            self.search_type        = None
            self.with_magnet        = None
            self.collection         = None
            self.database           = None
            self.crawler            = None
            self.with_db            = False 
            self.crawler_finished   = False 
            self.parser_finished    = False 
            self.soup_dict          = Queue.Queue()
            
            self.SetParameters(**kwargs)
            
            ## Initialising processes for crawing and parsing 
            self.Init()    
        except Exception as inst:
          ros_node.ParseException(inst)

    def Init(self):
        try:
            ## Generating one shot threads
            rospy.logdebug("=> Generating one shot threads")
            rospy.Timer(rospy.Duration(0.15), self.start_crawler, oneshot=True)
            rospy.Timer(rospy.Duration(0.15), self.start_parser,  oneshot=True)
            rospy.Timer(rospy.Duration(0.15), self.start_completer, oneshot=True)
            
        except Exception as inst:
          ros_node.ParseException(inst)

    def ConnectDB(self):
        try:
            if self.with_db:
                rospy.logdebug("  + Generating database [%s] in [%s] collections"% 
                                  (self.database, self.collection))
                
                if self.db_handler is None:
                    self.db_handler = MongoAccess(debug=False)
                    self.db_handler.connect(self.database, self.collection)

        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            return self.db_handler

    def SetParameters(self, **kwargs):
        try:  
            for key, value in kwargs.iteritems():
                if "title" == key:
                    self.title = value
                elif "page_limit" == key:
                    self.pages = value
                elif "search_type" == key:
                    self.search_type = value
                elif "with_magnet" == key:
                    self.with_magnet = value
                elif "collection" == key:
                    self.collection = value
                elif "database" == key:
                    self.database = value
                elif "with_db" == key:
                    self.with_db = value
        except Exception as inst:
            ros_node.ParseException(inst)

    def start_crawler(self, cond=None):
        try:
            while not rospy.is_shutdown():
                ## Waiting for next command
                with self.run_crawler:
                    rospy.logdebug("T1: Waiting for command to start crawler...")
                    self.run_crawler.wait()
                
                ## Starting crawler
                rospy.logdebug('T1: Starting crawler')
                self.crawler.get_html()
                self.crawler_finished = True
                

        except Exception as inst:
            ros_node.ParseException(inst)

    def start_parser(self, cond=None):
        try:
            while not rospy.is_shutdown():
                with self.run_parser:
                    rospy.logdebug("T2: Waiting for command to parser...")
                    self.run_parser.wait()
                
                ## Starting parser
                rospy.logdebug('T2: Starting parser')
                self.crawler.parse_html()
                self.parser_finished = True
                
                ## Call complete DB process
                rospy.logdebug('T2: Calling DB completion')
                self.run_complete()
        except Exception as inst:
          ros_node.ParseException(inst)

    def start_completer(self, cond=None):
        try:
            rate_sleep = rospy.Rate(1.0/5.0)
            while not rospy.is_shutdown():
                with self.run_post:
                    rospy.logdebug("T3: Waiting for command to complete DB...")
                    self.run_post.wait()

                ## Starting parser
                rospy.logdebug('T3: Starting post execution routine')
                rate_sleep.sleep()
                self.parser_finished    = False
                self.crawler_finished   = False

        except Exception as inst:
          ros_node.ParseException(inst)

    def run_complete(self):
        try:
            if self.crawler_finished and self.parser_finished:
                rospy.logdebug('  + Time to start post execution routine.. ')
                with self.run_post:
                    self.run_post.notifyAll()
            else:
                crawler_result = 'is finished' if self.crawler_finished else 'has NOT finished'
                parser_result = 'is finished' if self.parser_finished else 'has NOT finished'
                rospy.logdebug('  + Not started post execution routine because crawler %s and parser %s'%
                               (crawler_result, parser_result))

        except Exception as inst:
          ros_node.ParseException(inst)

    def collect_data(self, **kwargs):
        try:
            ## Clearing item counter
            
            ## Setting class parameters every time
            ##    a message comes
            self.SetParameters(**kwargs)
            
            ## Connecting to DB
            handler = self.ConnectDB()
            kwargs.update({'db_handler': handler})
            kwargs.update({'soup_dict': self.soup_dict})
            
            ## Created lime torrents crawler
            rospy.logdebug("=> Created lime torrent crawler")
            self.crawler = LimeTorrentsCrawler(**kwargs)
            
            with self.run_crawler:
                rospy.logdebug('  + Setting crawler')
                self.run_crawler.notifyAll()
            
            with self.run_parser:
                rospy.logdebug('  + Setting parser')
                self.run_parser.notifyAll()
            
        except Exception as inst:
          ros_node.ParseException(inst)

    def GetInvoice(self):
        invoice = None
        try:
            rospy.logdebug('  + Preparing service invoice')
            invoice = {
                    'database':          self.database,
                    'collection':        self.collection,
                    'search_type':       self.search_type,
                    'result':  {
                        'updated_items': self.crawler.updated_items,
                        'counted_pages': self.crawler.page_counter
                    }
                    
            }
        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            return invoice
