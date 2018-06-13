#!/usr/bin/env python

import sys, os
import pprint
import threading
import rospy
import datetime
import time
import json

from pprint import pprint
from optparse import OptionParser, OptionGroup
from hs_utils import ros_node
from find_latest import FindLatest

from std_msgs.msg import Bool
from std_msgs.msg import String
from torrent_search.msg import torrentQuery


class FinderNode(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## Use lock to protect list elements from
            ##    corruption while concurrently access. 
            ##    Check Global Interpreter Lock (GIL)
            ##    for more information
            self.threats_lock           = threading.Lock()
            
            ## This variable has to be started before ROS
            ##   params are called
            self.condition              = threading.Condition()

            ## Initialising parent class with all ROS stuff
            super(FinderNode, self).__init__(**kwargs)
            
            self.database          = None
            self.collection        = None
            self.controller        = None 
            self.search_type       = None 
            self.list_term         = None 
            self.slack_token       = None 
            self.slack_channel     = None 
            self.latest_days        = None 
            
            ## Parsing arguments
            for key, value in kwargs.iteritems():
                if "search_type" == key:
                    self.search_type = value

            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            if 'search_for_latest' in topic:
                ## Get incoming message
                self.database   = msg.database
                self.collection = msg.collection
                self.search_type= msg.search_type
    
                ## Notify thread that data has arrived
                with self.condition:
                    self.condition.notifyAll()
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def Init(self):
        try:
            ## TODO: If parameters are not given do not start
            ## Getting environment variables
            self.list_term = self.mapped_params['/torrent_finder/list_term'].param_value
            rospy.logdebug('+ Got list_term of [%s]'%str(self.list_term))
            self.slack_token = self.mapped_params['/torrent_finder/slack_token'].param_value
            rospy.logdebug('+ Got slack_token of [%s]'%str(self.slack_token))
            self.slack_channel = self.mapped_params['/torrent_finder/slack_channel'].param_value
            rospy.logdebug('+ Got slack_channel of [%s]'%str(self.slack_channel))
            self.latest_days = self.mapped_params['/torrent_finder/latest_days'].param_value
            rospy.logdebug('+ Got latest_days of [%s]'%str(self.latest_days))
            
            ## Starting publisher thread
            rospy.loginfo('Starting latest torrents service')
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
            
        except Exception as inst:
              ros_node.ParseException(inst)
 
    def Run(self, event):
        ''' Execute this method to... '''
        try:
            while not rospy.is_shutdown():
                with self.condition:
                    rospy.logdebug('+ Waiting for next command')
                    self.condition.wait()
                
                ## Collecting connection data
                args = {
                    'database':     self.database,
                    'collection':   self.collection,
                    'search_type':  self.search_type,
                    
                    'slack_token':  self.slack_token,
                    'slack_channel':self.slack_channel,
                    'list_term':    self.list_term,
                    'latest_days':  self.latest_days
                    }

                if self.search_type == 'find_latest':
                    rospy.loginfo('+ Getting latest records')
                    self.controller = FindLatest(**args)
                    newest_items = self.controller.GetMovies()
                
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

    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('finder_node', anonymous=False, log_level=logLevel)
        
    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~search_for_latest',  torrentQuery)
    ]
    pub_topics     = [
        #('~topic2',  Bool)
    ]
    system_params  = [
        '/torrent_finder/list_term',
        '/torrent_finder/slack_token',
        '/torrent_finder/slack_channel',
        '/torrent_finder/latest_days'
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = FinderNode(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()
