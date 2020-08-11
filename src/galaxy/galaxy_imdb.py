#!/usr/bin/env python

import sys, os
import threading
import rospy
import time
import Queue
import json
import pymongo

from optparse import OptionParser, OptionGroup
from pprint import pprint
from datetime import datetime, timedelta
from collections import defaultdict

from hs_utils import imdb_handler
from hs_utils import ros_node, logging_utils
from hs_utils import message_converter as mc
from hs_utils import json_message_converter as rj
from hs_utils.mongo_handler import MongoAccess
from galaxy_retrieve import TorrentParser
from std_msgs.msg import Int32

class ImdbCollector:
    
    def __init__(self, **kwargs):
        try:
            self.database           = None
            self.torrents_collection= None
            self.test_collection    = 'test'
            self.imdb_collection    = None
            self.latest_collection  = None
            self.torrents_db        = None
            self.imdb_db            = None
            self.test_db            = None
            self.imdb_handler       = None
            self.list_terms         = None
            self.time_span          = 6
            
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "torrents_collection" == key:
                    self.torrents_collection = value
                elif "imdb_collection" == key:
                    self.imdb_collection = value
                elif "latest_collection" == key:
                    self.latest_collection = value
                elif "list_terms" == key:
                    self.list_terms = value
                elif "time_span" == key:
                    self.time_span = value
                    rospy.logdebug('  Got time span [%d]'%self.time_span)

            ## Creating DB handler
            self.torrents_db = MongoAccess()
            connected       = self.torrents_db.Connect(
                                                self.database, 
                                                self.torrents_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.torrents_collection))
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.torrents_collection))
                
            ## Creating DB handler
            self.imdb_db = MongoAccess()
            connected       = self.imdb_db.Connect(
                                                self.database, 
                                                self.imdb_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.imdb_collection))
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.imdb_collection))
            
            ## Creating DB handler
            self.latest_db = MongoAccess()
            connected       = self.latest_db.Connect(
                                                self.database, 
                                                self.latest_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.latest_collection))
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.latest_collection))
                
            ## Creating DB handler
            self.test_db = MongoAccess()
            connected       = self.test_db.Connect(
                                                self.database, 
                                                self.test_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.test_collection))
            else: rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.test_collection))
            
            
            args = {
                'list_terms':self.list_terms,
                'imdb':      True
            }
            self.imdb_handler = imdb_handler.IMDbHandler(**args)
            rospy.loginfo("Created IMDb handler")
            
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def find_imdb_data(self, imdb_id, galaxy_id):
        dict_row = None
        existed  = False
        try:
            ## updating DB records with new item
            condition = { 'imdb_id' : imdb_id }
            ## Inserting record if it would not exists
            posts = self.imdb_db.Find(condition)
            
            ## Checking for latest
            posts_items = posts.count()
            if posts_items>0:
                rospy.logdebug("Record [%s] has [%d] items"%(imdb_id, posts_items))
                dict_row = posts[0]
#                 if posts_items>1:
#                     for p in posts:
#                         pprint(p)
#                         print "-- "*10
                    
                rospy.logdebug('  IMDB item [%s] was found locally'%imdb_id)
                
                ## TODO check if it is too old, renew it?
            else:
                rospy.loginfo("Collecting [%s]"%(imdb_id))
                dict_row = self.imdb_handler.get_info(imdb_id)
                
                ## inserting new record
                rospy.logdebug("Inserting in DB [%s]"%imdb_id)
                datetime_now = datetime.now() 
                dict_row.update({'last_updated' : datetime_now})
                result = self.imdb_db.Insert(dict_row)
                
                ## update latest timestamp that it was updated
                q = {"galaxy_id"    : galaxy_id}
                u = {"imdb_updated" : datetime_now }
                res = self.torrents_db.Update(condition=q, substitute=u)
                if not res: rospy.logdebug(' Not updated in [%s]'%galaxy_id)
                rospy.loginfo('Retrieved IMDB for [%s]'%imdb_id)
                
                ## double check if something went wrong while updating DB
                if not result: rospy.logwarn('Invalid DB update for [%s]'%dict_row['imdb_id'])
                existed  = True
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return dict_row, existed
  
    def search(self):
        try:
            query = { 
                'imdb_updated': False
            }
            
            sort_condition = [
                ("galaxy_id", pymongo.DESCENDING)
            ]
            posts  = self.torrents_db.Find(query, sort_condition=sort_condition)
            rospy.loginfo("Found [%d] torrents to update"%posts.count())
            
            for torrent in posts:
                
                ## searching for torrent in IMDB
                imdb_id = torrent['imdb_code']
                if not imdb_id:
                    rospy.logdebug("Missing IMDB id for [%s]"%(torrent['title']))
                    continue
                
                ## updating IMDB data
                dict_row, existed = self.find_imdb_data( imdb_id, torrent['galaxy_id'])
                if not dict_row:
                    rospy.logwarn("Invalid IMDB info collected")
                    continue
                
                ## setting update time if it was retrieved from IMDB web
                if not existed: 
                    datetime_now = datetime.now() 
                    torrent['imdb_updated'] = datetime_now

            rospy.loginfo("Finished searching IMDB info")
        except Exception as inst:
              ros_node.ParseException(inst)

    def merge_latest(self, parser=None):
        '''
        Collecting torrents with IMDB info
        '''
        if not parser:
            rospy.logwarn("Missing crawler object")
            return

        try:
            rospy.logdebug('Preparing infomration for latest torrents')
            date_latest = datetime.now() - timedelta(hours=self.time_span)
            query = { 
                '$and': 
                [
                    #{'torrent_updated': { '$exists': True}}, 
                    {'torrent_updated': { '$gte':    date_latest}} 
                ]
            };
            
            sort = [
                ("torrent_updated", pymongo.DESCENDING)
            ]
            ## search for latest page only and be sure to
            ## review ones with latest set IMDB information
            torrent_posts = self.torrents_db.Find(query, sort_condition=sort)
            rospy.loginfo("Found [%d] torrents to update"%torrent_posts.count())
            
            latest = defaultdict(lambda: {})
            
            for torrent in torrent_posts:
                #print "-"*120
                #pprint(torrent)
            
                ## assign given imdb code as index of torrent state
                galaxy_id = torrent['galaxy_id']
                t_imdb    = None

                if 'torrent_info' not in torrent:
                    rospy.logwarn('Record [%s] without torrent info'%galaxy_id)
                    continue
                torrent_info = torrent['torrent_info']
                
                ## assigning title to latest torrent record
                title     = torrent['title']
                if 'title' in torrent_info:
                    title = torrent_info['title']

                ## validate imdb code and get information
                if 'imdb_code' not in torrent or not torrent['imdb_code']:
                    rospy.logwarn('Torrent [%s] has no IMDB'%galaxy_id)
                else:
                    t_imdb   = torrent['imdb_code']
                    rospy.loginfo('Record [%s] has IMDB code [%s]'%(galaxy_id, t_imdb))
                    posts    = self.latest_db.Find({'imdb_code': t_imdb})
                    
                    ## IMDB information was not found in 
                    ## DB of latest seen torrents
                    if posts.count() < 1:
                        rospy.logdebug('Looking for IMDB code [%s]'%t_imdb)
                        imdb_data = parser.get_imdb_item(t_imdb)
                        if not imdb_data: rospy.logwarn('IMDB code not found!') 

                    else:
                        torrent_exists = False
                        for found in posts:
                            for item in found['torrents']:
                                if galaxy_id == item['galaxy_id']:
                                    torrent_exists = True
                                    break
                        
                        if torrent_exists:
                            rospy.logdebug('-  Found torrent [%s] in [%s]'%(t_imdb, galaxy_id))
                            continue
                    
                ## getting torrent data to display
                torrent_record = {
                    'title':        title,
                    'magnet':       torrent['magnet'],
                    'file':         torrent['file'],
                    'leechers':     torrent['leechers'],
                    'seeders':      torrent['seeders'],
                    'views':        torrent['views'],
                    'size':         torrent['size'],
                    'galaxy_id':    galaxy_id,
                }
                
                ## this element provides distinguishable 
                ## information about each torrent
                if 'torrent_info' in torrent:
                    torrent_record.update({'torrent_info': torrent['torrent_info']})

                ## prepare a collection with latest seen
                if torrent_posts.count()>0:
                    ## item already has a record and has to be updated
                    q = {'imdb_code': t_imdb};
                    u = {'torrents': torrent_record}
                    ok = self.latest_db.Update(condition=q, substitute=u, 
                            upsertValue=True, operator='$push')
            
                    ## double check if something went wrong while updating DB
                    if not ok: rospy.logwarn('Invalid DB update for [%s]'%t_imdb)
                    rospy.logdebug('-  Updated [%s] with [%s]'%(t_imdb, galaxy_id))
                    continue
                
                ## got first element of torrent IMDB
                if t_imdb:
                    imdb_info, e = self.find_imdb_data( t_imdb, galaxy_id )
                    if not imdb_info: rospy.logwarn("Invalid IMDB info collected")
        
                ## prepare record, use as index galaxy and IMDB ids
                new_record = {
                    'imdb_code':    t_imdb,
                    'torrents':    [torrent_record],
                    'imdb':         imdb_info,
                    'last_updated': datetime.now()
                }
        
                ## insert item as a latest seen torrent
                ok = self.latest_db.Insert(new_record)
                if not ok: rospy.logwarn('Item [%s] was not inserted'%(t_imdb))
                rospy.logdebug('+  Created [%s] with [%s]'%(t_imdb, galaxy_id))

        except Exception as inst:
            ros_node.ParseException(inst)

    def new_merge_latest(self, query, parser=None):
        '''
        Collecting torrents with IMDB info
        '''
        if not parser:
            rospy.logwarn("Missing crawler object")
            return

        try:
            rospy.logdebug('Preparing infomration for latest torrents')
            date_latest = datetime.now() - timedelta(hours=self.time_span)
            sort = [
                ("torrent_updated", pymongo.DESCENDING)
            ]
            ## search for latest page only and be sure to
            ## review ones with latest set IMDB information
            torrent_posts = self.torrents_db.Find(query, sort_condition=sort)
            rospy.loginfo("Found [%d] torrents to update"%torrent_posts.count())
            
            latest = defaultdict(lambda: {})
            total  = 0
            for torrent in torrent_posts:
                imdb_code = torrent['imdb_code']
                galaxy_id = torrent['galaxy_id']
                
                ## get imdb data
                imdb_item = {}
                if 'imdb_code' in torrent and imdb_code:
                    rospy.logdebug("  Torrent [%s] has IMDB, getting data of [%s]"%
                                   (galaxy_id, imdb_code))
                    imdb_data = parser.get_imdb_item(imdb_code)
                
                    ## there is no IMDB data for this torrent
                    if imdb_data.count()<1:
                        rospy.logdebug("  Torrent [%d] without IMDB data"%
                                   (imdb_data.count()))
                    else:
                        ## get first imdb item found for this torrent
                        if imdb_data.count()==1:
                            rospy.logdebug("  Found [%d] IMDB items for [%s]"%
                                           (imdb_data.count(), imdb_code))
                        elif imdb_data.count()>1:
                            rospy.logwarn("  Found [%d] IMDB items for [%s]"%
                                       (imdb_data.count(), imdb_code))
                        for p in imdb_data:
                            imdb_item = p
                            del imdb_item['_id']
                            break
                        
                ## Preparing merged item in latest torrents
                torrent.update({'imdb': imdb_item})
                
                ## inserting/updating item in DB
                q    = {'galaxy_id': galaxy_id}
                ok   = self.test_db.Update(condition=q,substitute=torrent,upsertValue=True)
                if not ok: rospy.logwarn('Item [%s] was not updated'%(t_imdb))
                else: rospy.logdebug('Updated [%s] in [%s.%s]'%
                                     (galaxy_id, self.database, self.test_collection))
                        
                ## remove id info
                del torrent['_id']
                total += 1
#                 if 0:
#                     print "-"*100
#                     pprint(torrent)
            rospy.loginfo("Updated [%s] items"% total)
            
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def close(self):
        try:
            self.torrents_db.Close()
            self.imdb_db.Close()
            self.latest_db.Close()
            self.test_db.Close()
        except Exception as inst:
            ros_node.ParseException(inst)

    def update_torrent(self, parser, galaxy_id):
        try:
            
            ## review ones with latest set IMDB information
            rospy.logdebug('Looking for torrent [%s]'%galaxy_id)
            q    = {'galaxy_id':galaxy_id}
            sort = [
                ("torrent_updated", pymongo.DESCENDING)
            ]
            posts= self.torrents_db.Find(q, sort_condition=sort)
            rospy.loginfo("Found [%d] torrents to update"%posts.count())
            
            for idx, post in enumerate(posts):
                parser.update_torrent(idx, torrent)
            
#             ## inserting/updating item in DB
#             sort = [("torrent_updated", pymongo.DESCENDING)]
#             ok   = self.test_db.Update(condition=q,substitute=torrent,upsertValue=True)
#             if not ok: rospy.logwarn('Item [%s] was not updated'%(t_imdb))
#             else: rospy.logdebug('Updated [%s] in [%s.%s]'%
#                                  (galaxy_id, self.database, self.test_collection))
        except Exception as inst:
            ros_node.ParseException(inst)

class GalaxyImdb(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            self.list_terms = None
            self.rate       = 5000
            self.time_span  = 6
            
            ## Initialising parent class with all ROS stuff
            super(GalaxyImdb, self).__init__(**kwargs)
            
            for key, value in kwargs.iteritems():
                if "rate" == key:
                    self.rate = value
                    rospy.logdebug('      Rate is [%d]'%self.rate)

            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)

    def Init(self):
        try:
            
            ## getting parameters
            self.list_terms= self.mapped_params['/galaxy_imdb/list_term'].param_value
            self.time_span = self.mapped_params['/galaxy_imdb/time_span'].param_value
            rospy.logdebug('  Got time span [%d]'%self.time_span)
            
            ## starting torrent parser
            args = {
                'database':           'galaxy',
                'latest_collection':  'latest',
                'torrents_collection':'torrents',
                'imdb_collection':    'imdb',
                'list_terms':          self.list_terms,
                'time_span':           self.time_span,
                'imdb':                True
            }                       
            self.crawler = ImdbCollector(**args)
            self.parser = TorrentParser(**args)
            
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
            rospy.Timer(rospy.Duration(0.5), self.OnDemand, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)

    def SubscribeCallback(self, msg, topic):
        try:
            if topic == '/galaxy_imdb/update_torrent':

                ## Storing message for queue
                rospy.logdebug('Got query message')
                stored_items = (topic, msg)
                self.queue.put( stored_items )
                
                ## Notify data is in the queue
                with self.condition:
                    self.condition.notifyAll()

        except Exception as inst:
            ros_node.ParseException(inst)

    def ShutdownCallback(self):
        try:
            rospy.logdebug('+ Shutdown: Closing torrent parser')
            if self.crawler:
                self.crawler.close()
                self.crawler = None
                
            rospy.logdebug('+ Shutdown: Closing torrent parser')
            if self.parser:
                self.parser.close()
                self.parser = None
        except Exception as inst:
              ros_node.ParseException(inst)

    def Run(self, event):
        ''' Run method '''
        try:
            rospy.logdebug('+ Starting run method')
            rate_sleep = rospy.Rate(1.0/self.rate)
            while not rospy.is_shutdown():

                #rospy.logdebug('  Looking for information in IMDB')
                #self.crawler.search()
                
                query = { 
                    '$and': [
                        {'imdb_updated': False},
                        {'reviewed' : {'$exists': 0}} 
                ]}
                self.parser.search(query)
                
                rospy.logdebug('  Merging latest torrents')
                date_latest = datetime.now() - timedelta(hours=self.time_span)
                query = { 
                    '$and': 
                    [
                        {'torrent_updated': { '$exists': True}}, 
                        {'torrent_updated': { '$gte':    date_latest}} 
                    ]
                };
                
                self.crawler.new_merge_latest(query, parser=self.parser)

                rate_sleep.sleep()
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def OnDemand(self, event):
        ''' Run method '''
        try:
            rospy.logdebug('+ Starting run method')
            while not rospy.is_shutdown():
                
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.loginfo('  Waiting for data to come...')
                    self.condition.wait()

                ## Check if there is something in the queue
                while not self.queue.empty():
                    (topic, msg) = self.queue.get()
                     
                query = {'galaxy_id':msg.data}
                rospy.logdebug('Updating torrent [%d]'%msg.data)
                self.parser.search(query)
                
                
                self.crawler.new_merge_latest(query, parser=self.parser)
        except Exception as inst:
              ros_node.ParseException(inst)

if __name__ == '__main__':
    usage       = "usage: %prog option1=string option2=bool"
    parser      = OptionParser(usage=usage)
    parser.add_option('--queue_size',
                type="int",
                action='store',
                default=1000,
                help='Topics to play')
    parser.add_option('--latch',
                action='store_true',
                default=False,
                help='Message latching')
    parser.add_option('--debug', '-d',
                action='store_true',
                default=True,
                help='Provide debug level')
    parser.add_option('--std_out', '-o',
                action='store_false',
                default=True,
                help='Allowing standard output')
    parser.add_option('--rate',
                action='store_false',
                default=14400.0,
                help='Period of time to crawl')

    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('galaxy_imdb', anonymous=True, log_level=logLevel)

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
         ('/galaxy_imdb/update_torrent',  Int32),
    ]
    pub_topics     = [
#         ('/event_locator/updated_events', WeeklyEvents)
    ]
    system_params  = [
        '/galaxy_imdb/list_term',
        '/galaxy_imdb/time_span',
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'allow_std_out':   options.std_out})
    args.update({'rate':            options.rate})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = GalaxyImdb(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

