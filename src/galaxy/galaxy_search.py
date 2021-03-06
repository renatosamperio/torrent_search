#!/usr/bin/env python

import sys, os
import threading
import rospy
import time
import json
import Queue
import requests
import re
import xmltodict, json

from collections import defaultdict
from optparse import OptionParser, OptionGroup
from pprint import pprint
from bs4 import BeautifulSoup
from datetime import datetime

from hs_utils import imdb_handler
from hs_utils import ros_node, logging_utils
from hs_utils.mongo_handler import MongoAccess
from hs_utils.title_info_parser import TitleInfoParser
from std_msgs.msg import Bool

class GalaxyCrawler:
    def __init__(self, **kwargs):
        try:
            self.imdb_handler = None
            self.database     = None
            self.collection   = None
            self.db_handler   = None
            self.parser       = None
            self.list_terms   = None
            self.imdb_handler = None
            
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value
                elif "list_terms" == key:
                    self.list_terms = value
                    
            if not self.list_terms:
                raise Exception("Missing list of terms")
            
            ## Creating DB handler
            self.db_handler = MongoAccess()
            connected       = self.db_handler.Connect(
                                                self.database, 
                                                self.collection)
            ## Checking if DB connection was successful
            if not connected:
                rospy.logwarn('DB not available')
            else:
                rospy.loginfo("- Created DB handler in %s.%s"%
                              (self.database, self.collection))
            
            ## creating imdb connection
            args = {
                'list_terms':self.list_terms,
                'imdb':      True
            }
            
            self.imdb_handler = imdb_handler.IMDbHandler(**args)
            rospy.logdebug("Created IMDb handler")
            
            ## creating parser of HTML crawling rows
            self.parser = defaultdict(lambda:[]) 
            self.parser.update({
                ## Type
                0: lambda col: self.get_type(col),
                
                ## Works?
                1: lambda col: None, #{ 'checked': col.find(title=True)['class']},
                
                ## Language
                2: lambda col: self.get_language(col),
                
                ## Torrent title and IMDB code
                3: lambda col: self.get_torrent_id(col),
                
                ## Torrent file and magnet
                4: lambda col: self.get_torrent_download(col),
                
                ## Rating
                5: lambda col: None,
                
                ## Uploader
                6: lambda col: None,
                
                ## Size
                7: lambda col: self.get_size(col),
                
                ## Comments
                8: lambda col: None,
                
                ## Views
                9: lambda col: self.get_views(col),
                
                ## Seeder/Leechers
                10: lambda col: self.get_seeders_leechers(col),
                
                ## Added
                11: lambda col: None,
                12: lambda col: None
            })
            rospy.loginfo('Created web crawler for galaxy torrent')
            
            self.title_info_parser = TitleInfoParser()
            rospy.loginfo('Created torrent info parser')
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def get_type(self, col):
        try:
            return { 
                'type': str(col.string.encode('ascii', 'ignore')) 
            }
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_language(self, col):
        try:
            return { 
                'language': str(col.find(title=True)['title'])
            }
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_torrent_id(self, col):
        info = None
        try:
            info = col.find(title=True)
            result = {
                        'title':     info['title'].encode('utf-8'),
                        'galaxy_id': '',
                        'imdb_code': ''
                    }
            imdb_code = list(col.find_all(href=True))
            
            href = str(info['href'])
            ## validating given ID
            if '/torrent' not in href:
                rospy.logwarn('Invalid torrent ID')
            else:
                galaxy_id = info['href'].split('/')[2]
                if galaxy_id.isnumeric():
                    galaxy_id = int(galaxy_id)
                else:
                    rospy.logwarn('Torrent ID not a number')
                result['galaxy_id'] = galaxy_id

            ## validting imdb code
            if len(imdb_code)>1:
                result['imdb_code']=str(imdb_code[1]['href'].split('=')[-1])

            return result
        except Exception as inst:
              ros_node.ParseException(inst)
              pprint(info)

    def get_torrent_download(self, col):
        try:
            items = list(col.find_all(href=True))
            magnet = items[1]['href'].encode('utf-8').strip()
            return {
                        'file':  str(items[0]['href']),
                        'magnet':str(magnet)
                    }
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_size(self, col):
        try:
            return { 
                'size': str(list(col.descendants)[-1]) 
            }
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_views(self, col):
        try:
            result = { 
                'views': list(col.descendants)[-1] 
            }
            result['views'] = result['views'].replace(",","")
            if result['views'].isnumeric():
                result['views'] = int(result['views'])
            else:
                rospy.logwarn('Views value is not a number: [%s]'%result['views'])
            return result
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def get_seeders_leechers(self, col):
        try:
            items = list(col.find_all(color=True))
            result = {
                'leechers':list(items[0].descendants)[-1],
                'seeders': list(items[1].descendants)[-1],
            }
            
            if result['seeders'].isnumeric():
                result['seeders'] = int(result['seeders'])
            else:
                rospy.logwarn('Leechers value is not a number')
            if result['leechers'].isnumeric():
                result['leechers'] = int(result['leechers'])
            else:
                rospy.logwarn('Leechers value is not a number')
            return result
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def validate_document(self, document):
        try:
            result = True
            if not document['galaxy_id']:
                rospy.logwarn('Invalid torrent id')
                result = False
            
            galaxy_id = document['galaxy_id']
            if 'https://' not in document['file']:
                rospy.logwarn('[%s] Torrent file not a valid URL: %s'%(galaxy_id, document['file']))
                result = False
            
            if 'magnet:' not in document['magnet']:
                rospy.logwarn('[%s] Invalid magnet: %s'%(galaxy_id, document['file']))
                result = False
                
            if not document['size']:
                rospy.logwarn('[%s] Invalid torrent size'%(galaxy_id))
                result = False
                
            if not document['type']:
                rospy.logwarn('[%s] Invalid torrent type'%(galaxy_id))
                result = False
                
            if not document['language']:
                rospy.logwarn('[%s] Invalid torrent language'%(galaxy_id))
                result = False
                
            if not document['title']:
                rospy.logwarn('[%s] Invalid torrent title'%(galaxy_id))
                result = False
            
            ## Not serious malformations
            if not document['leechers']:
                rospy.logdebug('[%s] Torrent without leechers'%(galaxy_id))
                
            if not document['seeders']:
                rospy.logdebug('[%s] Torrent without seeders'%(galaxy_id))
                
            if not document['views']:
                rospy.logdebug('[%s] Torrent without views'%(galaxy_id))
                
            if not document['imdb_code']:
                rospy.logdebug('[%s] Torrent IMDB code not included'%(galaxy_id))
            
            return result
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_torrent_info(self, torrent):
        try:
            ## preparing torrent id 
            torrent_info   = {}
            looked_titles  = []
            skipped_titles = 0
            galaxy_id = torrent['galaxy_id']
            imdb_code = torrent['imdb_code']
            
            ## getting extra information from torrent title
            title     = torrent['title'].replace('.', ' ').strip()
            mopped    = self.imdb_handler.clean_sentence(title).strip(' -')
            rospy.logdebug('Looking for extra data in [%s]'%title)
            torrent_info = self.title_info_parser.run(mopped, torrent_info)
            
            ## Assigning cleaned up title as torrent info
            if not torrent_info and len(imdb_code)>0:
                torrent_info = {'title': mopped}
                rospy.logdebug("Assigning clean title to existing IMDB");

            ## Adding torrent information
            if torrent_info:
                torrent.update({'torrent_info':torrent_info})
                rospy.logdebug('Updated item [%s]'%galaxy_id)
                mopped = torrent_info['title']
 
            if mopped not in looked_titles:
                    ## looking only once per item
                    looked_titles.append(title)
                    
                    ## looking for title in IMDB
                    rospy.logdebug ("TODO: Querying for imdb [%s]"%title)
                    
        except Exception as inst:
              ros_node.ParseException(inst)

    def search(self, url):
        result = False
        try:
            ## let's access latest torrents at GalaxyTorrent
            rospy.logdebug('  Requesting website')
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
            rospy.logdebug('  Parsing rows')
            rows = soup.find_all(class_="tgxtablerow")
            
            ## get new record 
            for i, row in enumerate(rows):
                
                ## build a DB record from scratching website
                cols = row.find_all(class_="tgxtablecell")
                dict_row = {}
                for j, col in enumerate(cols):
                    item = self.parser[j](col)
                    if item:
                        dict_row.update(item)

                ## check if prepared record is valid
                if not self.validate_document(dict_row):
                    rospy.logwarn('Invalid torrent: %s'%str(dict_row))
                    continue
                
                ## TODO: do not update elements that are the same?
                ##       or update everything but 'imdb_updated'
                ## adding imdb was collected
                dict_row.update({'imdb_updated'   : False})
                dict_row.update({'torrent_updated': datetime.now()})
                
                ## looking for more information about the torrent
                ## that is included in the torrent title
                self.get_torrent_info( dict_row )
                
                ## updating DB records with new item
                galaxy_id = dict_row['galaxy_id']
                condition = { 'galaxy_id' : galaxy_id }
                ok = self.db_handler.Update(condition, 
                                            dict_row, 
                                            upsertValue=True)
                if not ok: rospy.logwarn('Invalid DB update for [%s]'%galaxy_id)
                
                ## setting state result
                result = True
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return result

    def close(self):
        try:
            self.db_handler.debug = 0
            self.db_handler.Close()
        except Exception as inst:
            ros_node.ParseException(inst)

class GalaxySearch(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            self.list_terms = None            
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            self.rate       = 5000
            
            ## Initialising parent class with all ROS stuff
            super(GalaxySearch, self).__init__(**kwargs)
            
            for key, value in kwargs.iteritems():
                if "rate" == key:
                    self.rate = value
                    rospy.logdebug('      Rate is [%d]'%self.rate)

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
            
            ## getting parameters
            self.list_terms = self.mapped_params['/galaxy_imdb/list_term'].param_value
            
            kwargs = {
                'database':     'galaxy',
                'collection':   'torrents',
                'list_terms':   self.list_terms
            }
            self.crawler = GalaxyCrawler(**kwargs)
            
            rospy.Timer(rospy.Duration(0.5), self.Run,            oneshot=True)
            rospy.Timer(rospy.Duration(0.5), self.CollectOne,     oneshot=True)
            #rospy.Timer(rospy.Duration(0.5), self.CollectHistory, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def ShutdownCallback(self):
        try:
            rospy.logdebug('+ Shutdown: Closing torrent parser')
            if self.crawler:
                self.crawler.close()
                self.crawler = None
                
        except Exception as inst:
              ros_node.ParseException(inst)

    def Run(self, event):
        try:
            rospy.logdebug('+ Starting run method')
            while not rospy.is_shutdown():
                
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.logdebug('  Waiting for order...')
                    self.condition.wait()

                ## Check if there is something in the queue
                while not self.queue.empty():
                    (topic, msg) = self.queue.get()
                    
                rospy.loginfo('Updating latest torrents from GalaxyTorrent')
                url= 'https://torrentgalaxy.to/torrents.php?cat=41'
                ok = self.crawler.search(url)
                self.Publish('/galaxy_search/ready', Bool(ok) )
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def CollectOne(self, event):
        ''' Run method '''
        try:
            rospy.logdebug('+ Starting run method')
            rate_sleep = rospy.Rate(1.0/self.rate)
            while not rospy.is_shutdown():
                
                rospy.loginfo('Getting latest torrents from GalaxyTorrent')
                url= 'https://torrentgalaxy.to/torrents.php?cat=41'
                ok = self.crawler.search(url)
                self.Publish('/galaxy_search/ready', Bool(ok) )
                rate_sleep.sleep()
                
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def CollectHistory(self, event):
        ''' Run method '''
        try:
            ## Collect the next 10 pages and update IMDB code
            url= 'https://torrentgalaxy.to/torrents.php?cat=41&parent_cat=&sort=id&order=desc&page='
            search_pages = 10
            for page in range(1, search_pages):
                search_url = url+str(page)
                rospy.logdebug('Getting history in GalaxyTorrent page [%d]'%page)
                #ok = self.crawler.search(url)
            
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
    rospy.init_node('galaxy_search', anonymous=False, log_level=logLevel)
    
    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('/galaxy_search/do_search', Bool),
    ]
    pub_topics     = [
        ('/galaxy_search/ready',  Bool),
    ]
    system_params  = [
        '/galaxy_imdb/list_term',
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
        spinner = GalaxySearch(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

