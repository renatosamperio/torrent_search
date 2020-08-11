#!/usr/bin/env python

import sys, os
import threading
import rospy
import datetime
import time
import json
import re
import logging
import pymongo

from optparse import OptionParser, OptionGroup
from pprint import pprint
from imdb import IMDb
from datetime import datetime

from hs_utils import imdb_handler
from hs_utils import similarity
from hs_utils import ros_node, logging_utils
from hs_utils import message_converter as mc
from hs_utils import json_message_converter as rj
from hs_utils.mongo_handler import MongoAccess
from hs_utils.title_info_parser import TitleInfoParser
from hs_utils.utilities import compare_dictionaries

logging.getLogger('imdbpy.parser.http').setLevel(logging.getLevelName('DEBUG'))

class TorrentParser:
    def __init__(self, **kwargs):
        try:
            self.database           = None
            self.torrents_collection= None
            self.torrents_db        = None
            self.imdb_collection    = None
            self.imdb_db            = None
            self.imdb_handler       = None
            self.list_terms         = None
            self.title_info_parser  = None
            
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
                rospy.logdebug("Created DB handler in %s.%s"%
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
                rospy.logdebug("Created DB handler in %s.%s"%
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
                rospy.logdebug("Created DB handler in %s.%s"%
                              (self.database, self.latest_collection))
            
            ## creating imdb handler
            args = {
                'list_terms':self.list_terms,
                'imdb':      True
            }
            self.imdb_handler = imdb_handler.IMDbHandler(**args)
            rospy.logdebug("Created IMDb handler")
            rospy.loginfo("Created torrent parser")
            
            self.title_info_parser = TitleInfoParser()
            
            self.ia = IMDb()
            self.comparator = similarity.Similarity()
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def close(self):
        try:
            self.torrents_db.debug = 0
            self.imdb_db.debug     = 0
            self.latest_db.debug   = 0
            self.torrents_db.Close()
            self.imdb_db.Close()
            self.latest_db.Close()
        except Exception as inst:
            ros_node.ParseException(inst)

    def get_imdb_item(self, imdb_code):
        has_imdb = None
        try:
            
            ## look for the imdb item locally
            rospy.logdebug ("Searching for imdb [%s] locally"%imdb_code)
            has_imdb = self.imdb_db.Find({'imdb_id' : imdb_code})
            
            ## we didn't find it locally
            if has_imdb.count()<1:
                
                ## search for imdb item with another API
                has_imdb = self.imdb_handler.get_info(imdb_code)
                if has_imdb:
                    has_imdb['last_updated'] = datetime.now()
            
                    ## if imdb exists, we would like to  
                    ## keep this item locally
                    result = self.imdb_db.Insert(has_imdb)
                    if not result: rospy.logwarn('IMDB [%s] not updated'%imdb_code)
                    else: rospy.loginfo("Inserted in imdb DB [%s]"%imdb_code)
            else: 
                rospy.logdebug("Item [%s] already exists in imdb DB"%imdb_code)
            
        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            return has_imdb
        
    def search_extra_imdb(self, title, torrent={}):
        try:

            ## looking for title in an alternative IMDB API
            imdb_titles = self.ia.search_movie_advanced(title)
            rospy.loginfo ("Got [%d] items for imdb [%s]"%(len(imdb_titles), title))

            ## check if we found something informaiton
            has_imdb = None
            for item in imdb_titles:
                imdb_title = item.data['title']
                imdb_code  = 'tt'+item.movieID
                
                ## scoring collected data and assign perfect match
                score = self.comparator.score(title, imdb_title)
                if score == 1:
                    rospy.logdebug ("Searching for imdb [%s]"%imdb_code)
                    has_imdb = self.get_imdb_item(imdb_code)
                    torrent['imdb_code'] = imdb_code
                    
                    break
                elif score > 0.98:
                    rospy.loginfo('[%s] High similarity: [%s]'%(imdb_code, imdb_title))
                else:
                    rospy.loginfo('[%s] Low similarity:  [%s]'%(imdb_code, imdb_title))

        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            return has_imdb

    def update_torrent_info(self, torrent, title, torrent_info, post=None, ask=False):
        ok = False
        try:
            answer    = 'y'
            galaxy_id = torrent['galaxy_id']
            
            ## adding torrent information
            if not torrent_info:
                torrent['torrent_info'] = {"title": title}
                rospy.loginfo('[%s] has invalid torrent information'%galaxy_id)
            else: 
                torrent['torrent_info'] = torrent_info
                rospy.logdebug('[%s] has new torrent info'%galaxy_id)

            ## decide manually to update DB record
            if post and ask: 
                print '- - - - - torrent - - - - -'
                pprint(torrent)
                changes, summary = compare_dictionaries(
                    torrent, post, 
                    "torrent", "post", 
                    use_values=True)
                if len(changes)>0:
                    print ""
                    if len(summary['different'])>0:
                        print "DIFFERENT:"
                        for different in summary['different']:
                            print different
                    if len(summary['missing'])>0:
                        print "MISSING:"
                        for missing in summary['missing']:
                            print missing
            if ask: answer = raw_input("Do you want to update DB? ") or "y"
            if answer=='y':
                torrent['reviewed'] = datetime.now()
                query = {'galaxy_id':   galaxy_id}
                ok = self.torrents_db.Update(
                    condition=query, 
                    substitute=torrent, 
                    upsertValue=True)
                if not ok: rospy.logwarn('Invalid DB update for [%s]'%galaxy_id)
                else: rospy.loginfo('[%s] has updated torrent data'%galaxy_id)
            elif answer=='n':
                rospy.logdebug('[%s] not updated torrent in DB'%galaxy_id)
            if ask: print "-"*80

        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return ok

    def search(self, query, ask=False):
        try:
            sort = [
                ("galaxy_id", pymongo.DESCENDING)
            ]
            posts = self.torrents_db.Find(query, sort_condition=sort)
            if not posts: rospy.logwarn('Torrents not found')
            total = posts.count()
            rospy.loginfo('  Retrieved [%d] records'%total )
            
            looked_titles = []
            skipped_titles = 0
            for idx, post in enumerate(posts):
                
                torrent      = post.copy()
                keywords     = []
                torrent_info = {}
                group        = "***"
                
                ## getting extra information from torrent title
                galaxy_id    = torrent['galaxy_id']
                imdb_updated = torrent['imdb_updated']
                title        = torrent['title']
    
                ## torrent info has been already processed and it has
                ## an already cleaned title ready for an IMDB query
                if 'torrent_info' in torrent and torrent['torrent_info']:
                    torrent_info = torrent['torrent_info']
                    group = ""
                    if 'title' not in torrent_info:
                        rospy.logwarn('No title found for [%s]'%galaxy_id)
    
                ## clean up raw torrent title when there 
                ## is no torrent info without IMDB code
                title = self.imdb_handler.clean_sentence(title, keywords).strip(' -')
                
                ## whenever there are keywords it means that they were
                ## taken from the title and the title has changed too,
                ## therefore we should update title in torrent info
                if len(keywords)>0: 
                    torrent_info.update({'keywords':keywords, 'title': title})
    
                ## getting torrent info from applying filters
                rospy.loginfo('[%d/%d] Parsing [%s]'%(idx+1, total, title))
                torrent_info = self.title_info_parser.run(title, torrent_info)
                if 'title' in torrent_info.keys() and torrent_info: 
                    title = torrent_info['title']
                    rospy.logdebug('Using refreshed title [%s]'%title)
    
                ## searching for IMDB data
                has_imdb = self.search_extra_imdb(title,torrent=torrent)
    
                ## check if imdb info has been updated
                if has_imdb and not imdb_updated:
                    torrent['imdb_updated'] = datetime.now()
                    rospy.logdebug('[%s] has IMDB but it was not marked'%galaxy_id)
    
                ## updating torrent information in local DB
                ok = self.update_torrent_info(
                    torrent, title, torrent_info, post=post, ask=ask)
                #print ("%3d %10s \t %100s %s"%(idx+1, galaxy_id, title, group))

            ## tell how many were repeated
            rospy.logdebug('Skipped [%d] titles'%skipped_titles)

        except Exception as inst:
              ros_node.ParseException(inst)
 
class GalaxyRetrieve(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            self.condition  = threading.Condition()
            self.list_terms = None
            
            ## Initialising parent class with all ROS stuff
            super(GalaxyRetrieve, self).__init__(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)

    def Init(self):
        try:
            ## getting parameters
            self.list_terms = self.mapped_params['/galaxy_imdb/list_term'].param_value
            
            ## starting torrent parser
            args = {
                'database':           'galaxy',
                'latest_collection':  'latest',
                'torrents_collection':'torrents',
                'imdb_collection':    'imdb',
                'list_terms':         self.list_terms,
                'imdb':      True
            }                       
            self.parser = TorrentParser(**args)
            
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def ShutdownCallback(self):
        try:
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
            while not rospy.is_shutdown():
                
                rospy.logdebug('Querying  torrents')
                query ={ 
                    '$and': [
#                        {'torrent_info' : {'$exists': 1} },
                        {'reviewed' : {'$exists': 0} },    
                        {'imdb_code': ""  }
                ]}
                self.parser.search(query, ask=True)
                rospy.signal_shutdown("Finished")
            rospy.logdebug('+ Ended run method')
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
                default=False,
                help='Provide debug level')
    parser.add_option('--std_out', '-o',
                action='store_false',
                default=True,
                help='Allowing standard output')

    (options, args) = parser.parse_args()
    
    for k,v in  logging.Logger.manager.loggerDict.items():
        logging.getLogger(k).setLevel(logging.getLevelName('DEBUG'))
 
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('galaxy_retrieve', anonymous=False, log_level=logLevel)

    ## Defining static variables for subscribers and publishers
    sub_topics     = []
    pub_topics     = []
    system_params  = [
        '/galaxy_imdb/list_term',
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'allow_std_out':   options.std_out})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = GalaxyRetrieve(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

