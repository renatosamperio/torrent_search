#!/usr/bin/env python

import sys, os
import threading
import rospy
import datetime
import time
import json

from optparse import OptionParser, OptionGroup
from pprint import pprint

from hs_utils import ros_node, logging_utils
from hs_utils.mongo_handler import MongoAccess
from datetime import datetime

class UnlinkDownloads(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## Initialising parent class with all ROS stuff
            super(UnlinkDownloads, self).__init__(**kwargs)
            
            self.database   = None
            self.collection = None
            self.before     = None
            self.seed_limit = None
            ## Parsing arguments
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value
                elif "seed_limit" == key:
                    self.seed_limit = value
                    rospy.loginfo('Looking for items with more than [%d] seeds'%self.seed_limit)
                elif "before" == key:
                    self.before = value
                    rospy.loginfo('Looking for items after [%s]'%
                                  datetime.fromtimestamp(self.before))
            
            ## Creating DB handler
            self.db_handler = MongoAccess()
            connected       = self.db_handler.Connect(self.database, self.collection)
            ## Checking if DB connection was successful
            if not connected:
                rospy.logwarn('DB not available')
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.collection))
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
      
    def Init(self):
        try:
            
            query = { 
                '$and': 
                    [ 
                        { 'torrents.seeds': { '$gte': self.seed_limit } },
                        { 'torrents.state.status': 'finished' } 
                    ] 
            }
            rospy.loginfo('Searching for finished torrents')
            posts = self.db_handler.Find(query)

            ## Going along queried items
            for element in posts:
                for torrent in element['torrents']:
                    
                    ## Check if state is finished
                    if 'finished' in torrent['state']['status']:
                        hash    = torrent['hash']
                        history = torrent['state']['history']
                        rospy.logdebug('  Found torrent [%s]'%hash)
                        
                        last_update = -1
                        for item in history:
                            if item['operation'] == 'finished':
                                if last_update < item['last_update']:
                                    last_update = item['last_update']

                        ## Only looking for items after given time
                        if last_update >= self.before:
                            rospy.logdebug('  Ignoring torrent download at [%s]'%
                                   (datetime.fromtimestamp(item['last_update'])))
                            continue
                        rospy.loginfo('    Updating torrent [%s] from [%s]'%
                                   (hash, datetime.fromtimestamp(item['last_update'])))
                        
                        query = { 
                            "torrents": { 
                                '$elemMatch': { 
                                    'hash': hash
                                } 
                            } 
                        }
                        
                        condition = { "torrents.$.state.status": "unlinked" }
                        
                        updated_was_ok = self.db_handler.Update(query, condition)
                        label = 'updated successfully' if updated_was_ok else 'was not updated'
                        rospy.logdebug('    DB record [%s] %s'%(hash, label) )
                       #pprint(torrent)
                            
               # print "-"*100
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
    parser.add_option('--seed_limit',
                type="int",
                action='store',
                default=15000,
                help='Seeds greater than')
    parser.add_option('--before', 
                type="string",
                action='store',
                default=None,
                help='Search torrents before YYYY-MM-DD 24hh:mm:ss')

    (options, args) = parser.parse_args()
    
    if options.before is None:
        parser.error("Missing input varaible: --start in format YYYY-MM-DD 24hh:mm:ss" )
        sys.exit()
    else:
        try:
            date_from_time = datetime.strptime(options.before, "%Y-%m-%d %H:%M:%S")
            options.before = time.mktime(date_from_time.timetuple())
            
        except ValueError:
            print( 'Invalid date format %s, use YYYY-MM-DD 24hh:mm:ss'%
                   options.before)
            sys.exit(0)

        
    args            = {}
    logLevel        = rospy.DEBUG 
    rospy.init_node('band_search', anonymous=False, log_level=logLevel)
    
    ## Defining arguments
    args.update({'queue_size': options.queue_size})
    args.update({'before':     options.before})
    args.update({'seed_limit': options.seed_limit})
    args.update({'database':   'yts'})
    args.update({'collection': 'torrents'})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = UnlinkDownloads(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    #rospy.spin()

