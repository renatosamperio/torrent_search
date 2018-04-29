#!/usr/bin/env python

import sys, os
import pprint
import threading
import rospy
import datetime
import time
import json

from optparse import OptionParser, OptionGroup
from hs_utils import ros_node
from controller import TorrentsController

from std_msgs.msg import Bool
from std_msgs.msg import String
from torrent_search.msg import torrentQuery
from torrent_search.msg import torrentInvoice

class CrawlerNode(ros_node.RosNode):
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
            super(CrawlerNode, self).__init__(**kwargs)
            
            self.page_limit        = None
            self.with_magnet       = None
            self.search_type       = None
            self.with_db           = None
            self.title             = None
            self.database          = None
            self.collection        = None
            self.controller        = None 
            
            ## Parsing arguments
            for key, value in kwargs.iteritems():
                if "page_limit" == key:
                    self.page_limit = value
                elif "search_type" == key:
                    self.search_type = value
                elif "with_magnet" == key:
                    self.with_magnet = value
                elif "with_db" == key:
                    self.with_db = value

            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            if 'search_for_torrent' in topic:
                ## Get incoming message
                #with self.threats_lock:
                self.title      = msg.title
                self.database   = msg.database
                self.collection = msg.collection
                self.page_limit = msg.page_limit
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
            self.page_limit = self.mapped_params['/torrent_crawler/page_limit'].param_value
            rospy.logdebug('+ Got page_limit of [%s]'%str(self.page_limit))
            self.search_type = self.mapped_params['/torrent_crawler/search_type'].param_value
            rospy.logdebug('+ Got search_type of [%s]'%self.search_type)
            self.with_magnet = self.mapped_params['/torrent_crawler/with_magnet'].param_value
            rospy.logdebug('+ Got with_magnet of [%r]'%self.with_magnet)
            self.with_db = self.mapped_params['/torrent_crawler/with_db'].param_value
            rospy.logdebug('+ Got with_db of [%r]'%self.with_db)
            
            ## Starting publisher thread
            rospy.loginfo('Starting lime torrents crawler service')
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
            
            ## Initialising lime torrents crawler
            if self.controller is None:
                rospy.loginfo('+ Creating Lime torrent crawler')
                args = {}
                self.controller = TorrentsController(**args)
        except Exception as inst:
              ros_node.ParseException(inst)
 
    def Run(self, event):
        ''' Execute this method to... '''
        try:
            ## This sample produces calls every 250 ms (40Hz), 
            ##    however we are interested in time passing
            ##    by seconds
            rate_sleep = rospy.Rate(5) 
            
            while not rospy.is_shutdown():
                with self.condition:
                    rospy.logdebug('+ Waiting for next crawling command')
                    self.condition.wait()
                
                ## Collecting connection data
                args = {
                    'title':        self.title,
                    'page_limit':   self.page_limit,
                    'search_type':  self.search_type,
                    'with_magnet':  self.with_magnet,
                    'with_db':      self.with_db,
                    'database':     self.database,
                    'collection':   self.collection,
                    }

                if self.search_type == 'complete_db':
                    print "===> DO A COMPLETE DB ONLY"
                else:
                    rate_sleep = rospy.Rate(1.0)
                    rate_sleep.sleep()
                    self.controller.collect_data(**args)
                    
                    with self.controller.run_post:
                        rospy.loginfo('+ Waiting for post executioning actoin')
                        self.controller.run_post.wait()
                        
                        ## Get service invoice
                        rospy.loginfo('Calling invoice generation')
                        invoice = self.controller.GetInvoice()
                        self.PublishInvoice(invoice)
                    
        except Exception as inst:
              ros_node.ParseException(inst)

    def PublishInvoice(self, invoice):
        try:
            if invoice is None:
                rospy.logwarn('Invalid invoice')
                return
                
            ## Preparing invoice message
            msg = torrentInvoice()
            msg.database    = invoice['database']
            msg.collection  = invoice['collection']
            msg.service_type= invoice['search_type']
            msg.node_type   = 'torrent_crawler'
            msg.result      = json.dumps(invoice['result'], 
                                         sort_keys=True,
                                         separators=(',', ': '))
            msg.header.stamp= rospy.Time.now()
            self.Publish('service_result', msg)
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
    rospy.init_node('torrent_crawler', anonymous=False, log_level=logLevel)
        
    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~search_for_torrent', torrentQuery)
    ]
    pub_topics     = [
        ('service_result',     torrentInvoice)
    ]
    system_params  = [
        '/torrent_crawler/page_limit',
        '/torrent_crawler/search_type',
        '/torrent_crawler/with_magnet',
        '/torrent_crawler/with_db',
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = CrawlerNode(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()
