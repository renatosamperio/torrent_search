#!/usr/bin/env python

import sys, os
import threading
import rospy
import datetime
import time
import json
import Queue
import collections

from optparse import OptionParser, OptionGroup
from pprint import pprint

from hs_utils import ros_node, logging_utils
from hs_utils import message_converter as mc
from hs_utils import json_message_converter as rj
from hs_utils.mongo_handler import MongoAccess

from torrent_search.msg import YtsTorrents, SelectedTorrent

class TorrentFinder:
    
    def __init__(self, **kwargs):
        try:
            
            nothing = None
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def get_best_available(self, torrent_info):
        try:
            reason          = 'not chosen'
            selected_index  = -1
            magnet          = None
            id              = torrent_info.id
            max_size        = -1
            selected        = SelectedTorrent()
            selected.reason = None
            selected.magnet = None
            selected.id     = torrent_info.id
            
            ## Looking into selected torrents for best
            ##    item to collect the magnet and send
            ##    message for other node to download
            for index  in range(len(torrent_info.torrents)):
                torrent = torrent_info.torrents[index]
                
                ## Look for some other options in case 
                ##    torrent has low availability
                num_connections = torrent.seeds + torrent.peers
                if (num_connections) < 10: 
                    rospy.logdebug('Ignoring torrent with quality [%s]'%
                                  torrent.quality)
                    continue
                    
                ## Chossing the bigger file!
                if torrent.size_bytes > max_size:
                    selected_index = index
                    max_size       = torrent.size_bytes
                    reason         = 'Selected [%s] with quality of [%s]'%(
                        torrent_info.title_long, torrent.quality)
                    
                    ## Collecting magnet for this torrent
                    magnet = self.get_magnet(torrent_info, selected_index)
                    
                    ## Preparing data into a named tuple
                    selected.reason = reason
                    selected.magnet = magnet

            rospy.loginfo(selected.reason)
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return selected
        
    def get_magnet(self, torrent_info, index):
        magnet = None 
        try:
            if index < 0:
                rospy.loginfo('Not getting magnet, invalid index')
                return
            ## TODO: If magnet already exists do not call it again!
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
            
            magnet = 'magnet:?xt=urn:btih:%s&dn=%s&%s'% \
            (torrent_info.torrents[index].hash, encoded_movie_name, trackers)
            
            
            #for torrent in torrent_info.torrents:
            #    magnet = 'magnet:?xt=urn:btih:%s&dn=%s&%s'% \
            #    (torrent.hash, encoded_movie_name, trackers)
            
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return magnet

class ContentSelector(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## This variable has to be started before ROS
            ##   params are called
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            self.finder     = TorrentFinder()
            
            ## Initialising parent class with all ROS stuff
            super(ContentSelector, self).__init__(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            
            ## Storing message for queue
            rospy.logdebug('Got query message')
            stored_items = (topic, msg)
            self.queue.put( stored_items )
            
            ## Notify data is in the queue
            with self.condition:
                self.condition.notifyAll()

        except Exception as inst:
              ros_node.ParseException(inst)
      
    def Init(self):
        try:
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
            while not rospy.is_shutdown():
                
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.loginfo('  Waiting for data to come...')
                    self.condition.wait()

                ## Check if there is something in the queue
                while not self.queue.empty():
                    (topic, msg) = self.queue.get()
                    
                    ## Chossing torrent for downloading
                    for torrent in msg.torrent_items:
                        
                        ## Getting data into a named tuple
                        selected = self.finder.get_best_available(torrent)
                        if not selected.magnet:
                            rospy.logwarn('Invalid magnet collected for:')
                            rospy.logwarn(str(torrent))
                            continue

                        ## Publishing torrent to download
                        self.Publish('/yts_finder/torrents_to_download', selected)
                
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
    parser.add_option('--syslog',
                action='store_true',
                default=False,
                help='Start with syslog logger')

    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('content_selector', anonymous=False, log_level=logLevel)
    
    ## Sending logging to syslog
    if options.syslog:
        logging_utils.update_loggers()

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
         ('/yts_finder/found_torrents',       YtsTorrents),
    ]
    pub_topics     = [
         ('/yts_finder/torrents_to_download', SelectedTorrent)
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
        spinner = ContentSelector(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

