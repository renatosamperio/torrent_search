#!/usr/bin/python

import sys, os
import pprint
import rospy
import logging
import datetime
import time
import re

import numpy as np
from pprint import pprint
from hs_utils import ros_node 
from hs_utils import utilities 
from hs_utils import similarity 
from hs_utils.mongo_handler import MongoAccess
from hs_utils import slack_client
from optparse import OptionParser, OptionGroup

class MovieFinder:
    def __init__(self):
        ''' '''
        try: 
            self.comparator = similarity.Similarity()
        except Exception as inst:
            utilities.ParseException(inst)

    def list_values(self, new_item, key):
        try:
            key_item    = new_item[key]['value']
            item_month  = key_item.keys()[0]
            item_days   = key_item[item_month].keys()
            lst_values  = [key_item[item_month][day] for day in item_days]
            lst_values  = list(map(int, lst_values))
            array_values=np.array(lst_values)
            return array_values
        except Exception as inst:
          utilities.ParseException(inst)
    
    def clean_sentence(self, sentence, list_terms):
        try:
            ## Removing special works from torrent
            splitted        = sentence.strip().split()
            new_sentence    = []
            
            ## Split sentence and look if every word
            ##    is in the special list of characters
            ##    to remove.
            for token in splitted:
                if token.lower() not in list_terms:
                    new_sentence.append(token)
            new_sentence    = ' '.join(new_sentence)
            
            return new_sentence
        except Exception as inst:
          utilities.ParseException(inst)
    
    def get_imdb_best_title(self, torrent_info):
        try:
            from operator import itemgetter
            from imdbpie import Imdb
            
            imdb            = Imdb()
            updated_imdb    = []
            imdb_selected   = []
            item_selected   = {}
            year_found      = None
            torrent_title   = torrent_info['torrent_title']

            ## Using torrent title until year,
            ##   only if torrent title has a year
            title_has_year  = re.match(r'.*([1-3][0-9]{3})', torrent_title)
            if title_has_year is not None:
                year_found  = title_has_year.group(1)
                
                ## Adding torrent title year
                torrent_info.update({'year':year_found})
                splitted = str(torrent_title.split(year_found)[0])
            else:
                splitted = str(torrent_title)
                
            ## Remove non-required specific characters
            if not splitted.isalpha():
                splitted = splitted.translate(None, "([_#@&*|~`%^<>").strip()
            torrent_info['torrent_title'] = splitted
            
            ## Getting IMDB information
            imdb_data       = imdb.search_for_title(splitted)
            rospy.logdebug("+ Getting IMDB [%s] items"%(str(len(imdb_data))))
            
            ## Pre-selecting only IMDB titles that 
            ##   look very similar to torrent title
            ignored_items   = 0
            for imdb_item in imdb_data:
                score = self.comparator.score(splitted, imdb_item['title'])
                year_matches    = year_found == imdb_item['year']
                item_type       = 'feature' == imdb_item['type']
                
                ## Adding only very similar titles
                if score > 0.98:
                    imdb_item.update({'score':score})
                    imdb_item.update({'year_matches':year_matches})
                    updated_imdb.append(imdb_item)
                else:
                    ignored_items += 1
                    rospy.logdebug("      Ignored [%s] vs [%s] = %f"%
                                      (splitted, imdb_item['title'], score))

            rospy.logdebug("+   Ignored [%s] item(s)"%(str(ignored_items)))
            
            ## Sorting IMDB retrieved items by similarity score
            sorted_imdb = sorted(updated_imdb, key=itemgetter('score'), reverse=True) 
            
            ## Checking if torrent year matches, otherwise 
            ##   provide only feature type IMDB items
            better_item_not_found           = False
            for imdb_item in sorted_imdb:
                item_added                  = False
                new_item                    = {}
                if imdb_item['year_matches']:
                    better_item_not_found   = True
                    item_added              = True
                elif not better_item_not_found and 'feature' == imdb_item['type']:
                    item_added              = True
                    
                ## Retrieving additional IMDB information
                ##   and adding item
                if item_added:
                    imdb_id                 = imdb_item['imdb_id']
                    title_info              = imdb.get_title(imdb_id)
                    title_genres            = imdb.get_title_genres(imdb_id)
                    
                    ## Finding genre 
                    if title_genres is not None and 'genres' in title_genres:
                        genres_label         = str(', '.join(title_genres['genres']))
                        imdb_item.update({'genres': genres_label})

                    ## Finding image
                    imdb_image_url          = ''
                    if 'image' in title_info['base'].keys():
                        imdb_image_url          = title_info['base']['image']['url']
                    else:
                        rospy.logdebug("-   Image URL not found")
                        rospy.logdebug("-   Looking in similarities for images...")
                        for similarity_item in title_info['similarities']:
                            itemKeys        = similarity_item.keys()
                            if 'image' in itemKeys:
                                imageKey    = similarity_item['image'].keys()
                                if 'url' in imageKey:
                                    imdb_image_url = similarity_item['image']['url']
                                    rospy.logdebug("      Using image from similarities")
                                    break
                    ## Finding raiting
                    imdb_raiting            = ''
                    if 'rating' in title_info['ratings'].keys():
                        imdb_raiting        = str(title_info['ratings']['rating'])
                    else:
                        rospy.logdebug("-   Raiting not found")
                    
                    ## Finding movie plot
                    imdb_plot               = ''
                    if 'outline' in title_info['plot'].keys():
                        imdb_plot           = title_info['plot']['outline']['text']
                    else:
                        rospy.logdebug("-   Plot not found")

                    ## Creating data structure
                    imdb_title_url          = title_info['base']['id']
                    imdb_item.update({'raiting':    imdb_raiting})
                    imdb_item.update({'plot':       imdb_plot})
                    imdb_item.update({'image_url':  imdb_image_url})
                    imdb_item.update({'title_url':  'http://www.imdb.com/'+imdb_title_url})
                    imdb_selected.append(imdb_item)
            
            item_selected.update({'imdb_info':imdb_selected})
            item_selected.update({'torrent_info':torrent_info})
        except Exception as inst:
          utilities.ParseException(inst)
        finally:
            return item_selected

    def create_query(self, latest_days, item='seeds'):
        query = None
        try:
            ## Preparing day where item does not exists
            today           = datetime.datetime.now()
            stopper_date    = today - datetime.timedelta(days=latest_days)
            stopper_day     = str(stopper_date.day)
            stopper_month   = str(stopper_date.month)
            
            ## Preparing days where item started to exist
            stopper = { item+'.value.'+stopper_month+'.'+stopper_day: { '$exists': False }}
            query_items = [stopper]
            for i in range(latest_days):
                search_date = today - datetime.timedelta(days=latest_days-(i+1))
                search_month= str(search_date.month)
                search_day  = str(search_date.day)
                query_item  = { item+'.value.'+search_month+'.'+search_day: { '$exists': True }}
                query_items.append(query_item)
                
            query   = { '$and': query_items }
        except Exception as inst:
            utilities.ParseException(inst)
        finally:
            return query

class PostHelper:
    def prepare_fields(self, torrent_info, imdb_item):
        ''' 
        Makes a list of fields for slack message
        '''
        fields          = []
        try:
            label_seeds     = str(int(torrent_info['seeds']['mean']))
            label_leeches   = str(int(torrent_info['leeches']['mean']))
            lable_size      = str(torrent_info['size'])
            label_download  = '<'+torrent_info['download_link']+'|:arrow_down_small:>'
            label_raiting   = str(imdb_item['raiting'])
            label_genres    = imdb_item['genres']
            
            size           = {
                                "title": "Size",
                                "value": lable_size,
                                "short": True
                            }
            fields.append(size)
            if len(imdb_item['raiting'])>0:
                raiting_field= {
                                "title": "Raiting",
                                "value": label_raiting,
                                "short": True
                            }
                fields.append(raiting_field)
            seeds           = {
                                "title": "Seeds",
                                "value": label_seeds,
                                "short": True
                            }
            fields.append(seeds)
            leeches         = {
                                "title": "Leeches",
                                "value": label_leeches,
                                "short": True
                            }
            fields.append(leeches)
            genres          = {
                                "title": "Genres",
                                "value": label_genres,
                                "short": True
                            }
            fields.append(genres)
            dLink          = {
                                "title": "Download",
                                "value": label_download,
                                "short": True
                            }
            fields.append(dLink)
        except Exception as inst:
          utilities.ParseException(inst)
        finally:
            return fields
    
    def prepare_attachment(self, torrent_info, imdb_item, fields=None):
        ''' 
        Makes a list of attachments for slack message
        '''
        attachments          = []
        try:
            label_time_now  = datetime.datetime.now().strftime("Found on the %d of %B, %Y")
            label_torr_url  = str(torrent_info['torrent_link'])
            imdb_title      = str(imdb_item['title'])
            imdb_year       = str(imdb_item['year'])
            imdb_title_url  = str(imdb_item['title_url'])
            imdb_image_url  = str(imdb_item['image_url'])
            imdb_plot       = str(imdb_item['plot']).encode('utf-8').strip()
            
            if imdb_item['year'] is None:
                torrent_year        = str(torrent_info['year'])
                if torrent_year is not None or torrent_year != '':
                    attachment_title= imdb_title + " - " +torrent_year
            else:
                attachment_title = imdb_title + " - " + imdb_year
            attachement_item    = { "title":        attachment_title,
                                    "title_link":   imdb_title_url,
                                    "image_url":    imdb_image_url,
                                    
                                    "author_name": "Lime Torrents Crawler",
                                    "author_icon": "https://cdn.appmus.com/images/4bfa32737acbaaa618ef471b37099ad7.jpg",
                                    "author_link":  label_torr_url,
                                    
                                    "text":         imdb_plot,
                                    "pretext":      label_time_now,
                                    
                                    "footer":       "IMDB",
                                    "footer_icon":  'https://cdn4.iconfinder.com/data/icons/socialmediaicons_v120/48/imdb.png',
                                    
                                    "fields":       fields,
                                  }
            attachments.append(attachement_item)
        except Exception as inst:
          utilities.ParseException(inst)
        finally:
            return attachments
    
class FindLatest():
    def __init__(self, **kwargs):
        '''Service task constructor'''
        
        try:
            
            ## Adding local variables
            self.database         = None
            self.collection       = None
            self.db_handler       = None
            self.slack_channel    = None
            self.slack_token      = None
            self.latest_days      = None
            self.list_terms       = None
            self.movie_helper     = MovieFinder()
            self.post_helper      = PostHelper()
            self.slack_client     = None
            self.invoice          = None
            self.search_type      = None
            
            # Generating instance of strategy  
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "search_type" == key:
                    self.search_type = value
                elif "collection" == key:
                    self.collection = value
                elif "latest_days" == key:
                    self.latest_days = value
                elif "slack_token" == key:
                    self.slack_token = value
                    self.slack_client = slack_client.SlackHandler(self.slack_token)
                    rospy.logdebug("  +   Setting up slack client")
                elif "slack_channel" == key:
                    self.slack_channel = value
                    rospy.logdebug("  +   Setting up channel [%s]"%self.slack_channel)
                elif "list_term" == key:
                    if value is not None:
                        self.list_terms = self.LoadTerms(value)
            
            self.Init()
        except Exception as inst:
            utilities.ParseException(inst)
    
    def Init(self):
        try:
            rospy.logdebug("  + Connecting to [%s] with [%s] collections"% 
                                (self.database, self.collection))
            self.db_handler = MongoAccess()
            self.db_handler.connect(self.database, self.collection)
            
            rospy.logdebug('  + Registering invoice')
        except Exception as inst:
            utilities.ParseException(inst)

    def LoadTerms(self, fileName):
        try:
            with open(fileName, 'r') as file:
                return file.read().strip().split()
                
        except Exception as inst:
            utilities.ParseException(inst)

    def GetMovies(self):
        '''
        Finds latest movies appeared
        '''
        
        def mock_query():
            return { '$and': [
                { 'seeds.value.6.8': { '$exists': False }}, 
                { 'seeds.value.6.9': { '$exists': True }}, 
                { 'seeds.value.6.10':{ '$exists': True }}, 
                { 'seeds.value.6.11':{ '$exists': True }}
            ]}

        imdb_items   = []
        try:
            ## Getting Slack and IMDB clients
            slack_token         = self.slack_token
            slack_client        = SlackClient(slack_token)
            comparator          = Similarity.Similarity()
            for new_item in newest_items:
                
                ## Getting torrent data
                name            = new_item['name']
                clean_name      = clean_sentence(name)
                link            = new_item['link']
                size            = str(new_item['size'])
                seeds           = list_values('seeds')
                leeches         = list_values('leeches')
                magnet, dLink   = look_for_magnet(link)
                
                ## Torrent information
                torrent_info    = {
                    'torrent_title':    clean_name,
                    'size':             size,
                    'magnet':           magnet,
                    'download_link':    dLink,
                    'torrent_link':     link,
                    'seeds': {
                        'mean':         seeds.mean(),
                        'stdev':        seeds.std()
                    },
                    'leeches': {
                        'mean':         leeches.mean(),
                        'stdev':        leeches.std()
                    }
                }
                
                ## Searching for IMDB info
                imdb_selected   = get_imdb_best_title(torrent_info, comparator)
                if len(imdb_selected)<1:
                    rospy.logdebug("- No IMDB data found for [%s]"%clean_name)
                    continue
                
                ## Collect items to pass on
                imdb_items.append(items)

        except Exception as inst:
            utilities.ParseException(inst)
        finally:
            return imdb_items

    def PostMovies(self, imdb_items):
        try:
            db_size = self.db_handler.Size()
            rospy.logdebug("  + Getting [%s] records from [%s]"%(str(db_size), self.collection))
            
            ## Find all available records
#             query   = { '$and': [
#                 { 'seeds.value.4.8': { '$exists': False }}, 
#                 { 'seeds.value.4.9': { '$exists': True }}, 
#                 { 'seeds.value.4.10': { '$exists': True }}, 
#                 { 'seeds.value.4.11': { '$exists': True }}]}
            
            query = create_query(self.latest_days)
            pprint.pprint(query)
            
            records = self.db_handler.Find(query)
            rospy.logdebug("  + Found [%d] records"%records.count())
            for record in records:
                pprint.pprint(record)
        except Exception as inst:
            utilities.ParseException(inst)

                super_count += 1
            elapsed_time = time.time() - start_time
            rospy.logdebug("  + Collected [%d] records in [%s]"%(counter, str(elapsed_time)))
                
        except Exception as inst:
            utilities.ParseException(inst)
        finally:
            return all_changed_items, all_newest_items

if __name__ == '__main__':
    usage = "usage: %prog option1=string option2=bool"
    parser = OptionParser(usage=usage)
    parser.add_option('--database',
                type="string",
                action='store',
                default=None,
                help='Provide a valid database name')
    parser.add_option('--collection',
                type="string",
                action='store',
                default=None,
                help='Provide a valid collection name')
    parser.add_option('--list_term',
                type="string",
                action='store',
                default=None,
                help='List of terms to ignore')
    
    slack_parser = OptionGroup(parser, "Slack options",
                "Used to publish data in Slack")
    slack_parser.add_option('--slack_channel',
                type="string",
                action='store',
                default=None,
                help='Input a valid slack channel')
    slack_parser.add_option('--slack_token',
                type="string",
                action='store',
                default=None,
                help='Input a valid slack token')
  
    parser.add_option_group(slack_parser)
    (options, args) = parser.parse_args()
  
    if options.slack_token is None:
       parser.error("Missing required option: --slack_token='valid_slack token'")
    if options.slack_channel is None:
       parser.error("Missing required option: --slack_channel='valid_slack channel'")
    if options.list_term is None:
       parser.error("Missing required option: --list_term='valid_file_path'")
    if options.collection is None:
       parser.error("Missing required option: --collection='valid_collection'")
    if options.database is None:
       parser.error("Missing required option: --database='valid_database'")

    args = {}
    args.update({'database':        options.database})
    args.update({'collection':      options.collection})
    args.update({'list_term':       options.list_term})
    args.update({'slack_channel':   options.slack_channel})
    args.update({'slack_token':     options.slack_token})
    
    ## Executing task action for finding latest changes
    taskAction              = FindLatest(**args)
    changes, newest_items   = taskAction.GetMovies(['seeds'])
    #taskAction.PostNew(newest_items)