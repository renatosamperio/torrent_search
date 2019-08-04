#!/usr/bin/env python

'''
rostopic pub /yts_finder/trigger_download torrent_search/TriggerDownload "header: auto
seeds: 7000
search_type: 1" -1;

'''

import sys, os
import threading
import rospy
import datetime
import time
import json
import Queue
import copy

from optparse import OptionParser, OptionGroup
from pprint import pprint

from hs_utils import ros_node, logging_utils, utilities
from hs_utils import message_converter as mc
from hs_utils import json_message_converter as rj
from hs_utils.mongo_handler import MongoAccess
from torrent_search.msg import TriggerDownload
from torrent_search.msg import YtsTorrents, YtsTorrent
from torrent_search.msg import YtsHistory
from torrent_search.msg import YtsState

class YtsRecords(object):
    def __init__(self, **kwargs):
        try:
            self.database   = None
            self.collection = None
            
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value
                    
            ## Creating DB handler
            self.db_handler = MongoAccess()
            connected       = self.db_handler.Connect(
                                                self.database, 
                                                self.collection)
            ## Checking if DB connection was successful
            if not connected:
                rospy.logwarn('DB not available')
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.collection))
        except Exception as inst:
              ros_node.ParseException(inst)

    def search_not_finished_torrents(self, options):
        try:
            configured_options = options.keys()
            query = {}
            posts = None

            ## Search DB with uppers seeds limit
            if 'seeds' in configured_options:
                rospy.logdebug('Searching DB with uppers seeds limit')
                query = { 'torrents.seeds': { '$gte': options['seeds'] } }

            ## Search DB with given title
            elif 'query' in configured_options:
                rospy.logdebug('Search DB with given title')
                query = {'title': { '$regex': options['query'] } }

            ## Execute query
            posts     = self.db_handler.Find(query)
            ros_msg   = self.db_to_ros(posts)
                
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return ros_msg

    def db_to_ros(self, posts):
        '''
        Converts retrieved data into ROS messages
        '''
        def convert_list(field, message_type, item, give_list):
            '''
            Convert nested lists to ROS messages
            '''
            if len(item[field])<1:
                return
            
            try:
                for sub_item in item[field]:
                    sub_item = utilities.convert_to_str(sub_item)
                    converted_item = mc.convert_dictionary_to_ros_message(
                        message_type, 
                        sub_item)
                    give_list.append(converted_item)
            except Exception as inst:
                  ros_node.ParseException(inst)

        try:
            ros_msg = YtsTorrents()
            
            rospy.loginfo('Looking for [%d] retrieved items'%posts.count())
            ## Searching for retrieved posts
            for item in posts:
                item_keys       = item.keys()
                
                ## Removing nested items in a copy of from json message
                parsable_item = copy.deepcopy(item)
                del parsable_item['_id']
                del parsable_item['torrents']
                del parsable_item['hs_state']
                
                ## Paring item without nested structures
                parsable_item = utilities.convert_to_str(parsable_item)
                yts_torrent_info = mc.convert_dictionary_to_ros_message(
                                            "torrent_search/YtsTorrentInfo", 
                                            parsable_item) 
                
                ## Parsing torrents
                if 'torrents' in item_keys:
                    convert_list(  'torrents', 
                                   'torrent_search/YtsTorrent', 
                                   item,
                                   yts_torrent_info.torrents)

                ## Parsing HS state
                if 'hs_state' in item_keys:
                    convert_list(  'history', 
                                   'torrent_search/YtsHistory', 
                                   item['hs_state'],
                                   yts_torrent_info.hs_state.history)

                
                # Getting manget
                magnet = self.get_magnet( yts_torrent_info)
                
#                 for torrent in yts_torrent_info.torrents:
#                     pprint(torrent.magnet)

                ## Appending converted message
                ros_msg.torrent_items.append(yts_torrent_info)
            
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return ros_msg

    def get_magnet(self, torrent_info): #torrent_hash, encoded_movie_name):
        try:
            encoded_movie_name = torrent_info.title_long
            trackers = "tr=udp%3A%2F%2Fglotorrents.pw%3A6969%2Fannounce&" + \
            "tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80&" + \
            "tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969&" + \
            "tr=udp%3A%2F%2Fp4p.arenabg.com%3A1337&" + \
            "tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337&" + \
            "tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969&" + \
            "tr=udp%3A%2F%2Ftorrent.gresille.org%3A80%2Fannounce&" + \
            "tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&" + \
            "tr=udp%3A%2F%2Fopen.demonii.com:%3A1337%2Fannounce"
            
            for torrent in torrent_info.torrents:
                torrent.magnet = 'magnet:?xt=urn:btih:%s&dn=%s&%s'% \
                (torrent.hash, encoded_movie_name, trackers)
                
        except Exception as inst:
              ros_node.ParseException(inst)

class YtsFinder(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## This variable has to be started before ROS
            ##   params are called
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            self.client     = None
            
            ## Initialising parent class with all ROS stuff
            super(YtsFinder, self).__init__(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            ## Storing message for queue
            rospy.loginfo('Got query message')
            stored_items = (topic, msg)
            self.queue.put( stored_items )
            
            ## Notify data is in the queue
            with self.condition:
                self.condition.notifyAll()
                
        except Exception as inst:
              ros_node.ParseException(inst)
      
    def Init(self):
        try:
            args = {
                'database':     'yts',
                'collection':   'torrents'
            }
            self.client = YtsRecords(**args)
            
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def ShutdownCallback(self):
        try:
            rospy.logdebug('+ Shutdown: Doing nothing...')
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def Run(self, event):
        ''' Run method '''
        try:
            rospy.loginfo('Running YTS crawleser')
            while not rospy.is_shutdown():
                
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.loginfo('  Waiting to start search...')
                    self.condition.wait()

                ## Check if there is something in the queue
                while not self.queue.empty():
                    (topic, msg) = self.queue.get()

                    ## Get a list of not downloaded torrents
                    options = {}
                    
                    ## Looking for torrents with similar name
                    if msg.search_type ==  TriggerDownload.SEARCH_BY_TITLE:
                        options.update({'query': msg.title})
                        rospy.loginfo('Setting up title-based query')
                        
                    ## Looking for torrents with higher seeds
                    if msg.search_type ==  TriggerDownload.SEARCH_BY_SEEDS:
                        options.update({'seeds': msg.seeds})
                        rospy.loginfo('Setting up seeds-based query')
                        
                    ## Setting search query
                    msg = self.client.search_not_finished_torrents(options)
                    rospy.logdebug('Publishing parsed items')
                    
                    self.Publish('~found_torrents', msg)
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
    parser.add_option('--debug',
                action='store_true',
                default=False,
                help='Provide debug level')
    parser.add_option('--syslog',
                action='store_true',
                default=False,
                help='Start with syslog logger')

    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('yts_finder', anonymous=False, log_level=logLevel)
    
    ## Sending logging to syslog
    if options.syslog:
        logging_utils.update_loggers()

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~trigger_download',  TriggerDownload),
    ]
    pub_topics     = [
        ('~found_torrents',    YtsTorrents)
    ]
    system_params  = [
        #'/event_locator_param'
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    #args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = YtsFinder(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

