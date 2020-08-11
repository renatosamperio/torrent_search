#!/usr/bin/env python

import sys, os
import threading
import rospy
import datetime
import time
import json
import Queue

import libtorrent as lt

from optparse import OptionParser, OptionGroup
from pprint import pprint

from hs_utils import ros_node, logging_utils
from hs_utils import message_converter as mc
from hs_utils import json_message_converter as rj
from hs_utils.mongo_handler import MongoAccess

from torrent_search.msg import SelectedTorrent
from torrent_search.msg import YtsProgress

class Downloader:
    
    state_str = [
        'queued', 
        'checking', 
        'downloading metadata',
        'downloading', 
        'finished', 
        'seeding', 
        'allocating', 
        'checking fastresume'
    ]
            
    def __init__(self, **kwargs):
        try:
            self.ses              = lt.session()
            self.condition        = threading.Condition()
            
            ## Parsing local arguments
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value

            rospy.Timer(rospy.Duration(0.5), self.torrent_download, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)

    def set_torrent(self, selected):
        try:
            up_limit = 50*1000
            max_connections = -1
            params = { 
                'save_path':    '/opt/data/yts',
                'auto_managed': True
            }
            
            ## Setting magnet to download
            rospy.logdebug( "Setting torrent with ID [%s]"%selected.id)
            handle = lt.add_magnet_uri(
                self.ses, 
                selected.magnet, 
                params) 
            
            ## Setting up connection options
            handle.set_upload_limit(up_limit)
            handle.set_max_connections(max_connections)
            
            ## Waiting for metadata
            has_metadata = handle.has_metadata()

            ## Start torrent thread in case wouldn
            ##    not be running right now
            with self.condition:
                self.condition.notifyAll()
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_state(self, handle):
        try:
            status          = handle.status()
            s               = YtsProgress()
                
            ## Collecting torrent data
            s.handle_name   = handle.name()
            s.progress      = status.progress * 100.0
            s.download_rate = status.download_rate / 1000
            s.upload_rate   = status.upload_rate / 1000
            s.num_peers     = status.num_peers
            s.num_seeds     = status.num_seeds
            s.torrent_hash  = str(status.info_hash).upper()
            s.state         = self.state_str[status.state]
            s.save_path     = handle.save_path()
            s.is_finished   = status.is_seeding
            s.is_seeding    = status.num_seeds
            s.is_valid      = handle.is_valid()
            
            rospy.loginfo('(ALARM) %3.2f%% [down: %.1f kb/s, up: %.1f kB/s, peers: %d, seeds: %d] %s: [%s] '% (
                #self.alarm.elapsed_time,
                s.progress, 
                s.download_rate, 
                s.upload_rate, 
                s.num_peers, 
                s.num_seeds,
                s.state,
                s.handle_name,
            ))
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return s
    
    def torrent_download(self, event):
        ''' Simple download method'''
        try:
            ## Getting into downloading loop
            rate        = rospy.Rate(0.5)
            handles     = self.ses.get_torrents()
            num_handles = len(handles)
            
            while True:
                ## Looking if there are torrents to download
                ##    Otherwise wait for new messages
                if num_handles<1:
                    with self.condition:
                        rospy.loginfo('  Download thread is waiting...')
                        self.condition.wait()
            
                rospy.logdebug( "Starting timed download thread")
                
                ## Getting current handles
                handles     = self.ses.get_torrents()
                num_handles = len(handles)
                
                ## Looking for torrent queue
                for i in range(num_handles):
                    handle = handles[i]
                    yts_state = self.get_state(handle)
                        
                    rate.sleep()
        except Exception as inst:
              ros_node.ParseException(inst)

            
class DownloaderNode(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            ## Local objects
            self.downloader = None
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            
            ## Initialising parent class with all ROS stuff
            super(DownloaderNode, self).__init__(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:

            ## Storing message for queue
            rospy.logdebug('  Got selected torrent message')
            stored_items = (topic, msg)
            self.queue.put( stored_items )
            
            ## Notify data is in the queue
            with self.condition:
                self.condition.notifyAll()
            
        except Exception as inst:
              ros_node.ParseException(inst)
      
    def Init(self):
        try:
            ## Setting arguments for downloader
            args = {
                'database':   'yts',
                'collection': 'torrents'
            }
            
            ## Starting downloader process
            self.downloader = Downloader(**args)
            
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
            rospy.logdebug('+ Starting run method')
            while not rospy.is_shutdown():
                
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.loginfo('  Waiting for selected torrents...')
                    self.condition.wait()

                ## Check if there is something in the queue
                while not self.queue.empty():
                    (topic, msg) = self.queue.get()
                    
                    ## Appending new torrent
                    self.downloader.set_torrent(msg)

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
                help='Input debug level')
    parser.add_option('--std_out', '-o',
                action='store_false',
                default=True,
                help='Allowing standard output')

    (options, args) = parser.parse_args()

    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('downloader', anonymous=False, log_level=logLevel)
    
    ## Defining static variables for subscribers and publishers
    sub_topics     = [
         ('/yts_finder/torrents_to_download', SelectedTorrent)
    ]
    pub_topics     = []
    system_params  = []
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'allow_std_out':   options.std_out})
    #args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = DownloaderNode(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

