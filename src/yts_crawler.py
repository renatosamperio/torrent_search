#!/usr/bin/env python

import sys, os
import threading
import rospy
import datetime
import time
import json
import requests
import math 
import Queue

from optparse import OptionParser, OptionGroup
from pprint import pprint

from hs_utils import ros_node, logging_utils, utilities
from hs_utils.mongo_handler import MongoAccess
from std_msgs.msg import Bool

class YtsRequests(object):
    OK = 200
    
    def __init__(self, **kwargs):
        try:
            self.url = None
            self.database = None
            self.collection = None
            
            for key, value in kwargs.iteritems():
                if "url" == key:
                    self.url = value
                elif "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value
                    
            ## Creating DB handler
            self.db_handler = MongoAccess()
            connected       = self.db_handler.Connect(self.database, self.collection)
            ## Checking if DB connection was successful
            if not connected:
                rospy.logwarn('DB not available')
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.collection))
                
        except Exception as inst:
              ros_node.ParseException(inst)

    def insert_new_torrents(self, items):
        '''
        1) Find if item already exist in DB by item ID. Otherwise inserts it in DB.
        2) Looks if item is different and gets differences.
        3) If item is different, it updates item in DB with new item.
        '''
        try:
            for i in range(len(items)):
                item        = items[i]
                item_id     = item['id']
                condition   = {'id': item_id}
                
                ## Look if item by ID if it already exists
                posts       = self.db_handler.Find(condition)
                if posts.count() < 1:
                    
                    ## The query by ID did not return anything,
                    ##    therefore, we should create a new 
                    ##    torrent in DB
                    
                    ## Adding state per given torrent
                    for torrent in item['torrents']:
                        torrent.update({'state':{ 'status':'', 'history': []}})
                    
                    rospy.logdebug("Torrent item [%d] not found"%(item_id))
                    post_id = self.db_handler.Insert(item)
                    if post_id is not None:
                        rospy.loginfo("  Inserted item [%d]"%(item_id))
                else:
                    ## Compare new element to old one
                    rospy.logdebug("Torrent [%d] already exists"%(item_id))
                    old_item = posts[0]
                    new_item = item
                    
                    error, output =utilities.compare_dictionaries(
                            old_item, new_item, 
                            "stored", "new_item",
                            ignore_keys=['_id', 'hs_state'], 
                            use_values=True
                    )
                    
                    if len(error)<1:
                        rospy.logdebug("  Found item [%s] is similar to received message"%item_id)
                        continue
                    
                    ## Making a label whenever missing or different dictionaries
                    ##    are compared.
                    label = "with "
                    info_label = ''
                    if len(output['different'])>0:
                        label = "[%d] different item(s) "%len(output['different'])
                        info_label += 'different '
                    if len(output['missing'])>0:
                        if len(label)>0:
                            label += 'and '
                            info_label += 'and '
                        label += "[%d] missing item(s) "%len(output['missing'])
                        info_label += 'missing '
                    if len(label)>0:
                        rospy.loginfo('  Found %s'%label)
                        info_label += 'items'
                        ##pprint(output)
                    
                    ## Passing torrent state
                    new_item.update({
                        'hs_state': old_item['hs_state']
                    })
                    
                    ##Update new item
                    result = self.db_handler.Update(condition, new_item)
                    rospy.loginfo("  Updated item [%d] %s"%(item_id, info_label))
                    

        except Exception as inst:
              ros_node.ParseException(inst)

    def request_movie(self, url, payload, query_limit=None):
        try:
            rospy.loginfo('Checking page [%d]'%payload['page'])
            ## Request initial status
            
            coded_url = '?limit=%d&page=%d&with_rt_ratings=%s'%(payload['limit'], payload['page'],payload['with_rt_ratings'])
            response        = requests.get(url+coded_url) 
#             pprint(response.request.__dict__)
#             print "---> url:", (url+coded_url)
            if response.status_code != self.OK:
                rospy.logwarn("Reply Failed")
                pprint(payload)
                return None
                
            pages           = json.loads(response.content)
            torrents        = pages['data']
            status_message  = pages['status_message']
            status          = pages['status']
            limit           = torrents['limit']
            movie_count     = torrents['movie_count']
            page_number     = torrents['page_number']
            movies          = torrents['movies']
            
            if query_limit is not None:
                query_size  = query_limit
            else:
                query_size  = int(math.ceil(float(movie_count)/float(limit) ))
                        
            ## Assessing reply status
            if status != 'ok':
                rospy.logwarn(status_message)
                return None

            rospy.logdebug('  '+status_message)
            reply = {
                'query_size':   query_size,
                'movies':       movies,
                'page_number':  page_number,
                'next_page':    page_number+1,
                'limit':        limit,
                'movie_count':  movie_count,
                'query_size':   query_size,
            }
            rospy.logdebug("  Pulled "+str(page_number)+" of "+str(query_size) )
            return reply
        except Exception as inst:
              ros_node.ParseException(inst)

    def pull_movies(self, payload):
        try:
            retrieved_all_pages = True
            search_page         = payload['page']
            while retrieved_all_pages:
                payload['page']     = search_page
                yts_page            = self.request_movie(self.url, payload)#, query_limit=1)
                
                if yts_page is None:
                    rospy.loginfo('Should we wait 30s and retry? ' )
                    rospy.sleep(30)
                    continue
                    
                search_page         = yts_page['next_page']
                retrieved_all_pages = yts_page['next_page'] <= yts_page['query_size']
                
                rospy.loginfo("Got info from page %d/%d"%
                              (payload['page'], yts_page['query_size']))
                self.insert_new_torrents(yts_page['movies'])
#                 print "===> limit:\t\t\t",        yts_page['limit']
#                 print "===> movie_count:\t\t",    yts_page['movie_count']
#                 print "===> page_number:\t\t",    yts_page['page_number']
#                 print "===> query_size:\t\t",     yts_page['query_size']
#                 print "===> retrieved_all_pages:\t", retrieved_all_pages
#                 print "===> next_page:\t\t\t",    yts_page['next_page']
#                 print "===> movies.0.id:\t\t",    yts_page['movies'][0]['id']
#                 print "===> movies.size:\t\t",    len(yts_page['movies'])
            
        except Exception as inst:
              ros_node.ParseException(inst)
        
class YtsCrawler(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            self.client     = None
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            
            ## Initialising parent class with all ROS stuff
            super(YtsCrawler, self).__init__(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            ## Add item to queue
            #self.queue.put((topic, msg))

            ## Notify data is in the queue
            with self.condition:
                self.condition.notifyAll()
            
        except Exception as inst:
              ros_node.ParseException(inst)
      
    def Init(self):
        try:
            args = {
                'url':          'https://yts.lt/api/v2/list_movies.json',
                'database':     'yts',
                'collection':   'torrents'
            }
            self.client = YtsRequests(**args)
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
            rospy.logdebug('Running YTS crawleser')
            while not rospy.is_shutdown():
                payload = {
                    "limit":            50,
                    "page":             1,
                    "with_rt_ratings":  'true'
                }
                self.client.pull_movies(payload)
                
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.loginfo('  Waiting to start search...')
                    self.condition.wait()
            
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
    rospy.init_node('yts_crawler', anonymous=False, log_level=logLevel)
    
    ## Sending logging to syslog
    if options.syslog:
        logging_utils.update_loggers()

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~pull_messages',  Bool),
    ]
    pub_topics     = [
#         ('/event_locator/updated_events', WeeklyEvents)
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
        spinner = YtsCrawler(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

