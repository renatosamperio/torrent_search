#!/usr/bin/env python

'''
rostopic pub /download_torrent/start torrent_search/State "header: auto
magnet: 'magnet:?xt=urn:btih:5A4140BD59D66BCAC57CF05AF4A8FAB4EBCAE1C1&dn=Avengers: Endgame (2019)&tr=udp%3A%2F%2Fglotorrents.pw%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80&tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969&tr=udp%3A%2F%2Fp4p.arenabg.com%3A1337&tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969&tr=udp%3A%2F%2Ftorrent.gresille.org%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=udp%3A%2F%2Fopen.demonii.com:%3A1337%2Fannounce'
state: 'configure'" -1; 


rostopic pub /download_torrent/move_state torrent_search/State "header: auto
state: 'pause'" -1; 

DB Query
db.torrents.find( { "torrents": { $elemMatch: { hash: "97F58867E989E0DA30CFC56522B08A01646F27D1"} } } )
db.torrents.find( { "torrents": { $elemMatch: { hash: "97f58867e989e0da30cfc56522b08a01646f27d1".toUpperCase() } } } ).pretty()

db.copyDatabase("yts_12112019", "yts")

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
from hs_utils.mongo_handler import MongoAccess
from hs_utils import json_message_converter as rj
from torrent_search.msg import State
from torrent_search.msg import YtsTorrents
from std_msgs.msg import Float64
from hs_utils import slack_client

from transitions import Machine
import libtorrent as lt
from __builtin__ import True
from future.backports.test.pystone import TRUE

def set_history(operation):
    try:
        return {'operation': operation, 'last_update': time.time()}
    except Exception as inst:
        ros_node.ParseException(inst)

class Alarm:
    def __init__(self, **kwargs):
        try:
            ## Class variables
            self.is_finished                    = False
            self.is_running                     = False
            self.is_paused                      = False
            self.download_time                  = 0 #s
            self.download_pause                 = 0 #s
            
            ## Client callbacks
            self.client_stop_downloading        = None
            self.client_stop_pausing            = None
            
            ## timer
            self.elapsed_time                   = 0.0
            self.start_time                     = 0.0
            
            ## Parsing arguments
            for key, value in kwargs.iteritems():
                if "download_time" == key:
                    self.download_time = value
                elif "download_pause" == key:
                    self.download_pause = value
                elif "client_stop_downloading" == key:
                    self.client_stop_downloading = value
                elif "client_stop_pausing" == key:
                    self.client_stop_pausing = value
            
            rospy.loginfo('ALARM: Created alarm object')
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def start(self):
        try:
            
            self.is_finished = False
            rospy.logdebug('ALARM: Calling alarm timer')
            
            ## Executioning thread
            if not self.is_finished:
                rospy.logdebug('Calling start timer...')
                rospy.Timer(rospy.Duration(0.5), self.download_timer, oneshot=True)
            else:
                rospy.logdebug('Start timer not called, as it not finished')
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def stop(self):
        try:
            ## Executioning thread
            if not self.is_finished:
                rospy.Timer(rospy.Duration(0.5), self.pause_timer, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)

    def download_timer(self, event):
        try:
            self.start_time = time.time()
            self.elapsed_time = 0.0
            rate = rospy.Rate(4)
            rospy.logdebug('ALARM: Starting download alarm for %2.4fs'%self.download_time)
            self.is_running = True
            while not rospy.is_shutdown() and not self.is_finished:
                rate.sleep();
                
                self.elapsed_time = time.time() - self.start_time
                
                #print "===>download elapsed_time:", self.elapsed_time, self.download_time
                if self.elapsed_time> self.download_time:
                    break
            rospy.logdebug('ALARM: Stopped download timer after: %2.4fs'%self.elapsed_time)
            
            ## Calling client callback
            if self.client_stop_downloading is not None:
                self.client_stop_downloading()
            
            self.is_running = False
            ## Looping into pause timer
            self.stop()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def pause_timer(self, event):
        try:
            self.start_time = time.time()
            rate = rospy.Rate(4)
            rospy.logdebug('ALARM: Starting pause alarm for %2.4fs'%self.download_pause)
            self.is_paused  = True
            while not rospy.is_shutdown() and self.is_paused:
                rate.sleep();

                self.elapsed_time = time.time() - self.start_time
                #print "===>pause elapsed_time:", self.elapsed_time, self.download_pause
                if self.elapsed_time> self.download_pause:
                    self.is_paused = False
                    break
            rospy.logdebug('ALARM: Stopped pause timer after: %2.4fs'%self.elapsed_time)
            
            ## Calling client callback
            if self.client_stop_pausing is not None:
                self.client_stop_pausing()
            
            self.start()
        except Exception as inst:
              ros_node.ParseException(inst)
        
    def finish_now(self):
        try:
            self.is_finished = True
            rospy.logdebug('ALARM: Setting alarm to be finished')
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def has_time_finish(self):
        try:
            is_finished = self.elapsed_time > self.download_time
            time_finished = 'has finished' if is_finished else 'has not yet finished'
            rospy.logdebug('ALARM: Time %s'%time_finished)
            return is_finished
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def resume_download(self):
        try:
            self.is_paused = False
            rospy.logdebug('ALARM: Setting alarm to unapuse timer')
        except Exception as inst:
              ros_node.ParseException(inst)

class SlackPoster(object):
    def __init__(self, **kwargs):
        try:
            
            self.slack_client       = None
            self.slack_channel      = None
            self.channel_code       = None
            
            ## Startking slack client
            slack_token             = os.environ['SLACK_TOKEN']
            if not slack_token.startswith('xoxp'):
                rospy.logerr("Invalid slack token [%s]"%slack_token)
                rospy.signal_shutdown("Invalid slack token")
            
            rospy.logdebug("Got slack token and channel")
            self.slack_client = slack_client.SlackHandler(slack_token)
            
            ## Getting slack channel information
            try:
                self.slack_channel= os.environ['SLACK_CHANNEL']
                self.channel_code = self.slack_client.FindChannelCode(self.slack_channel)
                rospy.loginfo("Got channel code %s for name %s"%(self.channel_code, self.slack_channel))
            except KeyError:
                rospy.logerr("Invalid slack channel")
                rospy.signal_shutdown("Invalid slack channel")
                
        except Exception as inst:
            ros_node.ParseException(inst)
        
    def make_slack_message(self, content):
        try:
            
            rospy.logdebug('Creating slack message')
            content_keys = content.keys()
            for i in range(len(content_keys)):
                key = content_keys[i]
                content.update({ key: content[key] })
                rospy.logdebug('  Placed content from [%s]'%key)
            
            return [content]
            
        except Exception as inst:
            ros_node.ParseException(inst)

    def post_state(self, torrent_state, hash, target_state):
        try:
            for torrent in  torrent_state['torrents']:
                if torrent['hash'] == hash:

                    ## Colour selection
                    color    = '#ff0000'
                    if torrent_state['state'] == 'ok':
                        color = '#36a64f'

                    ## Making up content to show up
                    content = {
                        
#                             "fallback":    '',
#                             "author_name": '',
#                             "author_link": '',
#                             "author_icon": '',
#                             
#                             'pretext':      "",
                         'title':        torrent_state["title_long"],
                         'title_link':   "https://www.imdb.com/title/"+torrent_state["imdb_code"]+"/",
                         'text':         torrent_state["description_full"],
                        
                         "color":        color,
                         "image_url":    torrent_state["large_cover_image"],
                         "footer":       "YTS",
                         "footer_icon":  "https://yts.lt/assets/images/website/og_yts_logo.png",
                         "ts":           int( time.time() ),
                        
                         'fields': [
                             
                             {
                                 "title": "Genres",
                                 "value": ', '.join(torrent_state["genres"]),
                                 "short": True
                             },
                             {
                                 "title": "Language",
                                 "value": torrent_state["language"],
                                 "short": True
                             },
                             {
                                 "title": "Rating",
                                 "value": torrent_state["rating"],
                                 "short": True
                             },
                             {
                                 "title": "Runtime",
                                 "value": torrent_state["runtime"],
                                 "short": True
                             },
                             {
                                 "title": "Date Uploaded",
                                 "value": torrent_state["date_uploaded"],
                                 "short": True
                             },
                             {
                                 "title": "MPA Rating",
                                 "value": torrent_state["mpa_rating"],
                                 "short": True
                             },
                             {
                                 "title": "Quality",
                                 "value": torrent["quality"],
                                 "short": True
                             },
                             {
                                 "title": "Size",
                                 "value": torrent["size"],
                                 "short": True
                             },
                             {
                                 "title": "Seeds",
                                 "value": torrent["seeds"],
                                 "short": True
                             },
                             {
                                 "title": "Peers",
                                 "value": torrent["peers"],
                                 "short": True
                             },
                             {
                                 "title": "Type",
                                 "value": torrent["type"].title(),
                                 "short": True
                             },
                             {
                                 "title": "State",
                                 "value": target_state.title(),
                                 "short": True
                             }
                         ]
                    }
                    
                    ## Posting message
                    slack_message = self.make_slack_message(content)
                    
                    response = self.slack_client.PostMessage(
                        self.slack_channel, "Torrent Download",
                        attachments = slack_message,
                        as_user     = False,
                     
                    )

                    ## Something went wrong in the posting
                    if not response['ok'] :
                        if response['error'] == 'ratelimited':
                            ## Put things back into queue but wait for a second...
                            rospy.loginfo('Slack messages are being posted too fast in channel %s'%
                                          self.slack_channel)
                            element = (channel, text, attachment, msg_id)
                            self.slack_bag.queue.appendleft(element)
                            rospy.logdebug('ADD:   waiting for %ds'%wait_time)
                            rospy.sleep(wait_time)
                        else:
                            ## We do not know what to do in other case
                            rospy.logwarn("Slack posting went wrong: %s"%response['error'])
                            pprint(response)
                    else:
                        rospy.loginfo('Slack message was posted for [%s]'%torrent_state["title_long"])

        except Exception as inst:
              ros_node.ParseException(inst)

class Downloader(object):
    def __init__(self, **kwargs):
        try:
            self.ses                = None
            self.params             = None
            self.torrents_tracker   = {}
            
            ## Automatic start and stop
            seconds_per_hour        = 3600
            self.download_time      = 0.5* seconds_per_hour #s
            self.download_pause     = 6.0* seconds_per_hour #s
            rospy.logdebug('Starting alarm')
            
            for key, value in kwargs.iteritems():
                if "download_time" == key:
                    self.download_time = value
                    rospy.logdebug('  Setting download_time to [%f]'%self.download_time)
                elif "download_pause" == key:
                    self.download_pause = value
                    rospy.logdebug('  Setting download_pause to [%f]'%self.download_pause)
                    
            args = {
                'download_time':            self.download_time,
                'download_pause':           self.download_pause,
                'client_stop_downloading':  self.client_stop_downloading,
                'client_stop_pausing':      self.client_stop_pausing
            }
            self.alarm = Alarm(**args)
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def client_stop_pausing(self):
        try:
            rospy.loginfo('client_stop_pausing: nothing to do')
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def client_stop_downloading(self):
        try:
            rospy.loginfo('client_stop_downloading, nothing to do')
        except Exception as inst:
              ros_node.ParseException(inst)

class MetaDataDownloader(object):
    def __init__(self, **kwargs):
        
        try:
            self.torrent_info     = None
            self.index            = None
            self.downloader       = None
            self.session          = None
            self.has_requested    = False
            self.id               = None
            self.torrents_tracker = {}
            
            for key, value in kwargs.iteritems():
                if "torrent_info" == key:
                    self.torrent_info = value
                elif "index" == key:
                    self.index = value
                elif "downloader" == key:
                    self.downloader = value
                elif "id" == key:
                    self.id = value
                    
            
            self.torrent_data = self.torrent_info['torrents'][self.index]
            
            self.trigger_metadata_download()
        except Exception as inst:
              ros_node.ParseException(inst)
        
    def download_torrent_info(self, event):
        try:
            session         = self.downloader.ses
            params          = self.downloader.params
            up_limit        = self.downloader.up_limit
            max_connections = self.downloader.max_connections

            if session is None:
                rospy.logwarn('  [%d] Session not ready to download torrent info'%self.id)
                self.torrents_tracker['metadata']['has_metadata'] = False
                return
                
            if params is None:
                rospy.logwarn('  [%d] Parameters not yet defined to add magnet'%self.id)
                self.torrents_tracker['metadata']['has_metadata'] = False
                return
            
            ## Preparing to download torrent
            handles     = session.get_torrents()
            num_handles = len(handles)
            handle      = lt.add_magnet_uri(session, self.torrent_data['magnet'], params) 
            rospy.logdebug('  [%d] Adding magnet URI, current handles [%d]'%(self.id, num_handles))
                        
            rospy.loginfo('  [%d] Downloading metadata, current handles [%d]'%(self.id, num_handles))
            metadata_trigger_time = time.time()
            
            ## Collecting downloading data
            self.torrents_tracker['metadata']['retries'] += 1
            self.torrents_tracker['metadata']['retry_ts'] = time.time()
            
            ## Downloading data
            metadata_expired = True
            has_metadata = handle.has_metadata()
            while (not has_metadata and metadata_expired): 
                self.torrents_tracker['metadata']['is_searching'] = True
                time.sleep(0.5)
                metadata_expired = (time.time() - metadata_trigger_time) <= 60
                has_metadata = handle.has_metadata()
                #print "---> metadata_expired:", metadata_expired, has_metadata
                
            label = 'has metadata'
            if not has_metadata:
                label = 'expired without metadata'
            rospy.loginfo('  [%d] Tracker %s'%(self.id, label))
            
            ## Setting up connection options
            handle.set_upload_limit(up_limit)
            handle.set_max_connections(max_connections)
            
            ## All went fine!
            self.torrents_tracker['metadata']['has_metadata'] = has_metadata
            self.torrents_tracker['metadata']['is_searching'] = False
            self.has_requested = True

        except Exception as inst:
              ros_node.ParseException(inst)

    def trigger_metadata_download(self):
        try:
            torrent_data = self.torrent_info['torrents'][self.index]
            hash = torrent_data['hash']
            
            torrent_name = self.torrent_info['title_long']+'-'+torrent_data['quality']+'-'+torrent_data['type']
            rospy.logdebug('  [%d] Looking for torrent metadata for [%s]'%(self.id, torrent_name))
            
            ## Setting up local state
            if hash not in self.downloader.torrents_tracker:
                rospy.logdebug('  [%d] Added tracking locally for %s'%(self.id, hash))
                self.torrents_tracker.update({
                        'hash':             hash,
                        'state':            'search metadata',
                        'name':             torrent_name,
                        'metadata':{
                            'has_metadata': False,
                            'retries':      0,
                            'retry_ts':     None,
                            'is_searching': False
                        }
                     
                })
            else:
                rospy.loginfo('  [%d] Tracker  [%s] already in local state'%(self.id, torrent_name))
                            
            timer = rospy.Timer(rospy.Duration(0.025), self.download_torrent_info, oneshot=True )
            return timer
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def has_metadata(self):
        return self.torrents_tracker['metadata']['has_metadata']

    def is_searching(self):
        return self.torrents_tracker['metadata']['is_searching']
    
    def is_finished(self):
        return self.has_requested

class TorrentDownloader(Downloader):
    def __init__(self, **kwargs):
        try:
            
            super(TorrentDownloader, self).__init__(**kwargs)

            self.state_str = [
                    'queued', 
                    'checking', 
                    'downloading metadata',
                    'downloading', 
                    'finished', 
                    'seeding', 
                    'allocating'
            ]
            
            rospy.logdebug('Starting torrent session')
            self.ses                = lt.session()
            self.chosen_torrents    = []
            self.handle             = None
            self.status             = None
            self.not_complete       = True
            self.close_now          = True
            self.download_started   = False
            self.previous_state     = 'None'
            self.database           = None
            self.collection         = None
            
            ## Parsing arguments
            for key, value in kwargs.iteritems():
                if "save_path" == key:
                    self.save_path = value
                elif "up_limit" == key:
                    self.up_limit = value
                elif "max_connections" == key:
                    self.max_connections = value
                elif "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value

            self.params = { 'save_path': self.save_path}
            
            ## Creating DB handler
            self.db_handler = MongoAccess()
            connected       = self.db_handler.Connect(self.database, self.collection)
            ## Checking if DB connection was successful
            if not connected:
                rospy.logwarn('DB not available')
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.collection))
            
            ## Create slack poster
            self.slack_poster = SlackPoster()
            
            ## This variable has to be started before ROS
            ##   params are called
            self.fsm_condition  = threading.Condition()
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def get_state_str(self, state): 
        try:
            if state < len(self.state_str):
                return self.state_str[state]
            else:
                rospy.loginfo('Unrecognised state (%s): %s'%(str(state), str(state)) )
                return str(state).strip()
        except Exception as inst:
              ros_node.ParseException(inst)

    def set_up_configuration(self, torrent=None):
        try:
            if torrent is None:
                rospy.logwarn ('Setting up torrent failed as no torrent info was provided')
                ## TODO: Send it to error state?
                self.fail()
                return
            
            ## FSM transition status
            rospy.logdebug('---> Setting up configuration ['+self.previous_state+'] -> ['+self.state+']')
            timers = []
            
            ## Looking for selected torrents
            for id in range(len(torrent['torrent_items'])):
                torrent_info = torrent['torrent_items'][id]
                rospy.loginfo('Setting up [%s] to download'%torrent_info['title_long'])
                for i in range(len(torrent_info['torrents'])):
                    torrent_data     = torrent_info['torrents'][i]
                    selected_torrent = torrent_data['state']['status'] == 'selected'
                    is_downlading    = self.is_downloading_torrent(torrent_data['hash'])
                    
                    if (selected_torrent and not is_downlading ):
                        
                        ## Creating a metadata download thread
                        rospy.logdebug('Downloading metadata in a separate thread')
                        args = {
                            'torrent_info': torrent_info, 
                            'index': i, 
                            'session': self.ses,
                            'downloader': self,
                            'id' : id
                        }
                        downloader = MetaDataDownloader(**args)
                        timers.append(downloader)
                        
                        ## Assigning meta data thread 
                        if torrent_data['hash'] not in self.torrents_tracker:
                            rospy.logdebug('Tracking state locally for %s'%torrent_data['hash'])
                            self.torrents_tracker.update({
                                torrent_data['hash']: {
                                    'state': '   search_metadata',
                                    'name':      None,
                                    'metadata':  downloader
                                }
                            })

                        time.sleep(0.25)
            downloading_torrents=len(timers)
            rospy.loginfo('Metadata downloading for [%d] torrents'%downloading_torrents)

            ## Waiting to download metadata for timers...
            if downloading_torrents>0:
                rospy.loginfo('Waiting to download metadata')
                rate = rospy.Rate(1.0)
                
                all_are_finished = False
                label = ''
                while not all_are_finished:
                    rate.sleep()
                    for timer in timers:
                        all_are_finished = timer.is_finished()
                        if all_are_finished: 
                            label = 'not '
                            break
                
                rospy.logdebug('Metadata %scollected'%label)

            ## Resume download quickly to add torrent
            if self.is_paused():
                rospy.loginfo('Download not started, pausing again session...')
                self.ses.pause()
                self.pause()
                return
            
            ## Moving to next state
            rospy.logdebug('Starting download')
            self.download()
            
        except Exception as inst:
            ros_node.ParseException(inst)
            self.fail()
     
    def start_downloading(self): 
        try:
            ## Validating current FSM state
            if not self.is_Downloading():
                self.failed_transition()
            elif self.previous_state != 'Paused':
                
                ## Update new state in DB and locally
                rospy.logdebug('  Updating downloading status for existing handlers ['+self.previous_state+'] -> ['+self.state+']')
                handles = self.ses.get_torrents()
                    
                for handle in handles:
                    status = handle.status()
                    state  = self.get_state_str(status.state)
                    hash = str(status.info_hash).upper()
                    self.update_db_state(hash, state)
                        
                ## Verify is session isn't already downloading stuff...
                if not self.download_started:
                    self.download_started = True
                    rospy.logdebug('---> Starting to download ['+self.previous_state+'] -> ['+self.state+']')
                    
                    ## Downloading thread
                    rospy.Timer(rospy.Duration(0.5), self.downloader_thread, oneshot=True)
                else:
                    rospy.logdebug('---> Download thread already started  ['+self.previous_state+'] -> ['+self.state+']')
            else:
                 rospy.loginfo('Downloading from paused state')
                 
            ## Keeping last state
            self.previous_state = self.state
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def close_torrent(self): 
        try:
            rospy.logdebug('---> Closing torrent ['+self.previous_state+'] -> ['+self.state+']')
            self.download_started = False
            self.restart()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def pausing_download(self): 
        try:
            if not self.is_Paused():
                self.failed_pause()
            else:
                rospy.logdebug('---> Pausing download')
                self.ses.pause()
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def resume(self, torrent=None): 
        try:
            if not self.is_Paused():
                self.failed_pause()
            else:
                rospy.logdebug('---> Resume download ['+self.previous_state+'] -> ['+self.state+']')

            if self.alarm.is_paused:
                rospy.logdebug('Stopping pausing timer, starting downloading timer')
                self.alarm.resume_download()

            ## Resume session
            self.ses.resume()

        except Exception as inst:
              ros_node.ParseException(inst)

    def unlocking(self):
        try:
            ## Do not resume download if downloader thread
            ##    is paused because of the time
            rospy.loginfo('---> Unlocking downloading thread ['+self.previous_state+'] -> ['+self.state+']')
            
            
            if not self.is_paused():
                ## Notify data is in the queue
                rospy.logdebug('Notifying download loop!')
                with self.fsm_condition:
                    self.fsm_condition.notifyAll()

            ## Moving to next state
            self.download()

        except Exception as inst:
              ros_node.ParseException(inst)

    def halted_add(self, torrent=None):
        try:
            ## Do not resume download if downloader thread
            ##    is paused because of the time
            rospy.logwarn('---> Halted torrent update ['+self.previous_state+'] -> ['+self.state+']')
            
            
            if self.is_paused():
                rospy.logwarn("Stop the pause timer!")
                self.alarm.resume_download()
            
        except Exception as inst:
              ros_node.ParseException(inst)
        
    def failed_transition(self): 
        try:
            rospy.logdebug('---> Failed transition ['+self.previous_state+'] -> ['+self.state+']')
            self.previous_state = self.state
            self.reset()
        except Exception as inst:
              ros_node.ParseException(inst)

    def reset_error(self): 
        try:
            self.previous_state = self.state
            rospy.logdebug('---> Recovering from error ['+self.previous_state+']')
        except Exception as inst:
              ros_node.ParseException(inst)

    def print_info(self, handle):
        try:
        
            status          = handle.status()
            handle_name     = handle.name()
            print "===> handle_name:", handle_name
            print "===> status.info_hash:", status.info_hash
            print "===> status.progress:", status.progress
            print "===> status.is_finished:", status.is_finished
            print "===> status.is_seeding:", status.is_seeding
            print ""
            print "===> status.total_download:", status.total_download
            print "===> status.total_payload_download:", status.total_payload_download
            print "===> status.total_done:", status.total_done
            print "===> status.total_wanted_done:", status.total_wanted_done
            print "===> status.total_wanted:", status.total_wanted
            print ""
            print "===> status.progress:", status.progress
            print "===> status.finished_time:", status.finished_time
            print "===> status.active_time:", status.active_time
            print "===> status.seeding_time:", status.seeding_time
            print "===> status.progress_ppm:", status.progress_ppm
            print ""
            print "===> COND:",  status.is_seeding and status.is_finished
            print "===> status.state:", self.get_state_str(status.state)
            print "===> "*5
    
        except Exception as inst:
            ros_node.ParseException(inst)

    def is_downloading_torrent(self, hash):
        try:
            return hash in self.torrents_tracker.keys()
        except Exception as inst:
            ros_node.ParseException(inst)
        
    def downloader_thread(self, event):
        ''' Run method '''
        try:
            ## FSM transition status
            rospy.logdebug('---> Downloading ['+self.previous_state+'] -> ['+self.state+']')

            rospy.logdebug('  Updating downloading status for all handlers')
            handles = self.ses.get_torrents()
            for handle in handles:
                status = handle.status()
                state  = self.get_state_str(status.state)
                hash = str(status.info_hash).upper()
                self.update_db_state(hash, state)

            ## Starting timed download alarm
            handles = self.ses.get_torrents()
            num_handles = len(handles)
            if num_handles>0:
                self.alarm.start()
            
            while num_handles>0:
                if self.state == 'Paused':
                    with self.fsm_condition:
                        rospy.loginfo('  Download has been halted')
                        self.fsm_condition.wait()
                
                ## Looking for each handle
                handles     = self.ses.get_torrents()
                num_handles = len(handles)
                
                ## Maximum rate limited to 10 items per second
                num_handles = 10 if num_handles >10 else num_handles
                
                ## Look for each torrent
                for i in range(num_handles):
                    rate            = rospy.Rate(0.5)
                    handle          = handles[i]
                    status          = handle.status()
                    handle_name     = handle.name()
                    
                    ## Collecting torrent data
                    progress        = status.progress * 100.0
                    download_rate   = status.download_rate / 1000
                    upload_rate     = status.upload_rate / 1000
                    num_peers       = status.num_peers
                    num_seeds       = status.num_seeds
                    torrent_hash    = str(status.info_hash).upper()
                    state           = self.get_state_str(status.state)
                    
                    ## Looking if torrent ID exists locally
                    if torrent_hash not in self.torrents_tracker.keys():
                        rospy.logdebug('Keeping locally torrent state for %s'%handle_name)
                        self.torrents_tracker.update({
                            torrent_hash: {
                                'state': state,
                                'name': handle_name
                            }
                        })
                    ## Updating state if it has changed while downloading
                    elif self.torrents_tracker[torrent_hash]['state'] != state:
                        if self.torrents_tracker[torrent_hash]['name'] is None:
                            self.torrents_tracker[torrent_hash]['name'] = handle_name

                        rospy.logdebug('State of [%s] has changed from [%s] to [%s] while downloading'%
                                       (self.torrents_tracker[torrent_hash]['name'], 
                                        self.torrents_tracker[torrent_hash]['state'], state))

                        self.update_db_state(torrent_hash, state)
                        self.torrents_tracker[torrent_hash]['state'] = state
                        
                    ## Updating torrents that had not download metadata
                    if state == 'downloading metadata':
                        time_last_update = time.time() - tracker.torrents_tracker['metadata']['retry_ts']
                        if time_last_update > 60.0:
                            rospy.logwarn('Torrent [%s] metadata not downloaded after [%s]'%
                                          (tracker.torrents_tracker['name'], time_last_update))
                        rospy.loginfo('[%s] is %s'%(handle_name, state))
                        tracker = self.torrents_tracker[torrent_hash]['metadata']
                        pprint(tracker.torrents_tracker)
                        rospy.logdebug("===> name: %s" % tracker.torrents_tracker['name'])
                        rospy.logdebug("===> is_finished: %s" %str( tracker.is_finished()) )
                        rospy.logdebug("===> has_metadata:  %s"%str( tracker.has_metadata()) ) 
                        rospy.logdebug("===> time_last_update: %f" %( time_last_update))
#                         {'hash': u'4853EF54A80C624481C9021EB933FE9D20D9795C',
#                          'metadata': {'has_metadata': False,
#                                       'is_searching': True,
#                                       'retries': 1,
#                                       'retry_ts': 1568147504.86465},
#                          'name': u'Tone-Deaf (2019)-720p-web',
#                          'state': 'search metadata'}
                        continue

                    previous_state  = self.previous_state
                    current_state   = self.state
                    
                    rospy.loginfo('(%4.2f) %3.2f%% [down: %.1f kb/s, up: %.1f kB/s, peers: %d, seeds: %d] %s: [%s] '% (
                        self.alarm.elapsed_time,
                        progress, 
                        download_rate, 
                        upload_rate, 
                        num_peers, 
                        num_seeds,
                        state,
                        handle_name,
                    ))
                    
                    ## Remove torrent if it is already finished, 
                    ##    otherwise keep pulling for state
                    if status.is_seeding and status.is_finished:
                        rospy.loginfo('[%s] has just finished'%handle_name)
                        
                        ## Removing handle from session
                        self.ses.remove_torrent(handle)
                        rospy.loginfo('Removing handle [%s] from session'%handle_name)
                        
                        ## Update state in DB
                        self.update_db_state(torrent_hash, 'finished')
                        
                        ## Stop alarm timer if time has finished and no more 
                        ##    torrents are available
                        handles     = self.ses.get_torrents()
                        num_handles = len(handles)
                        
                        if num_handles<1:
                            rospy.logdebug('Stopping downloading timer alarm')
                            self.alarm.finish_now()
                        
                        
                        ## Removing torrent local info
                        rospy.logdebug('Removing local info for [%s]'%
                                       self.torrents_tracker[torrent_hash]['name'])
                        del self.torrents_tracker[torrent_hash]
                        break
                    else:
                        rate.sleep()
                
            
            ## Moving to next state
            rospy.loginfo( "Download(s) finished")
            
            ## Resetting download state
            self.download_started = False
            self.done()
            return
        except Exception as inst:
              ros_node.ParseException(inst)

    def remove_torrents(self):
        try:
            i = 0
            while i < len(self.chosen_torrents):
                torrent_state = self.chosen_torrents[i]
                
                ## Updating current torrent history and state
                for torrent in  torrent_state['torrents']:

                    ## Remove from local memory any finished torrent
                    if 'state' in torrent.keys() and torrent['state']['status'] == 'finished':
                        del self.chosen_torrents[i]
                        break
                i += 1

        except Exception as inst:
            ros_node.ParseException(inst)

    def update_db_state(self, hash, target_state, debug_=False):
        def has_state_in_history(state, history):
            try:
                for item in history:
                    if item['operation'] == state:
                        return False
                    rospy.logdebug('    State ignored [%s]'%str(item['operation']))
                
                rospy.logdebug('    State [%s] not found'%state)
                return True
            except Exception as inst:
                ros_node.ParseException(inst)
        
        try:
            target_state = target_state.strip()
            if len(target_state)<1:
                rospy.logwarn('Not possible to update DB, invalid target')
                return
            
            ## Getting specific torrent thta will be update in DB
            rospy.logdebug("  Let's update DB [%s] state for %s"%(target_state, hash))
            
            query = { 
                "torrents": { 
                    '$elemMatch': { 
                        'hash': hash
                    } 
                }
            }
            
            posts = self.db_handler.Find(query)
            for element in posts:
            
                ## Updating current torrent history and state
                for i in range(len( element['torrents'])):
                    
                    ## We start to look for an specific hash
                    if element['torrents'][i]['hash'] != hash:
                        continue
                        
                    ## 1) Update state in DB
                    ## Create state in DB record
                    new_state = set_history(target_state)
                    if 'state' not in element['torrents'][i].keys():
                        rospy.logdebug('  Creating [%s] state DB state for [%s]'%(target_state, hash))
                        state = {
                            'state': {
                                'status': target_state,
                                'history': [new_state]
                            }
                        }
                        element['torrents'][i].update(state)
                        rospy.loginfo('  Created new DB state [%s] for [%s]'%
                                      (target_state, element['id']))
                        updated_was_ok = self.db_handler.Update(query, element)
                    elif has_state_in_history(target_state, element['torrents'][i]['state']['history']):
                            
                        ## Updated new status in already defined state
                        rospy.logdebug('  Updated state [%s] DB state for [%s]'%(target_state, hash))
                        element['torrents'][i]['state']['status'] = target_state
                        element['torrents'][i]['state']['history'].append( new_state )

                        ## Update state according to existing hash
                        query = {'id': element['id']}
                        rospy.loginfo('  Updating DB state [%s] for [%s]'%
                                      (target_state, element['id']))
                        updated_was_ok = self.db_handler.Update(query, element)
                        label = 'updated successfully' if updated_was_ok else 'was not updated'
                        rospy.logdebug('    DB record [%s] %s'%(hash, label) )
                        
                    else:
                        rospy.logdebug('    Queried DB state [%s] is already defined for item [%s]'%
                                      (target_state, hash))
                        

                    ## 2) Update state in a local copy
                    ## Creating local state if it would not exist
                    item_not_found = True 
                    for torrent_state in self.chosen_torrents:
                        if 'id' in torrent_state.keys() and torrent_state['id'] == element['id']:
                            rospy.logdebug("  Item with ID [%s] is already in local copy"%element['id'])
                            item_not_found = False
                            break
                    if item_not_found:
                        rospy.logdebug("  No local copy of torrents for [%s]"%hash)
                        self.chosen_torrents.append(element)
                    
                    if debug_: pprint(element)
                    
                    ## 3) Updating slack status
                    if target_state == 'finished':
                        self.slack_poster.post_state(element, hash, target_state)

            #pprint(self.chosen_torrents)
            ## Updating local state
            rospy.logdebug("  Let's update local [%s] state for [%s]"%(target_state, hash))
            for i in range(len(self.chosen_torrents)):
                torrent_state = self.chosen_torrents[i]
                
                ## Updating current torrent history and state
                for torrent in  torrent_state['torrents']:
                    if torrent['hash'] == hash:
                        if torrent['state']['status'] != target_state:
#                             
                            torrent['state']['status'] = target_state
                            torrent['state']['history'].append( set_history(target_state) )
                            rospy.logdebug('    Updated local state [%s] updated for [%s]'%(target_state, hash))
                        else:
                            rospy.logdebug('    Local state [%s] already updated for [%s]'%
                                           (target_state, hash))
                        break
                    
            ## Remove finished torrents from local memory
            if target_state == 'finished':
                self.remove_torrents()

        except Exception as inst:
              ros_node.ParseException(inst)

    def is_torrent_finished(self, torrent_info):
        try:
            ## Looking if torrent has any downloaded file
            query = { "id": torrent_info['id'] }
            posts = self.db_handler.Find(query)
            for element in posts:
                for torrent in element['torrents']:
                    
                    ## Find finished downloaded torrents
                    if 'state' in torrent.keys() and torrent['state']['status'] == 'finished':
                        return True

            return False
        except Exception as inst:
              ros_node.ParseException(inst)

    def client_stop_pausing(self):
        try:
            if self.is_Paused():
                rospy.loginfo('Re-starting torrents download')
                self.unpause()
                
                
                rospy.logdebug('Notifying download loop!')
                with self.fsm_condition:
                    self.fsm_condition.notifyAll()

            else:
                rospy.logwarn('Not re-starting, current state is [%s]'%self.state)
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def client_stop_downloading(self):
        '''
        This method is called by client, and should not interact directly with 
        unpause mechanism. It should advance state and in the transition it 
        should be managed how to unpause the timer
        '''
        try:
            rospy.logdebug('---> Stopping alarm threads ['+self.previous_state+'] -> ['+self.state+']')
            if self.is_Downloading():
                rospy.loginfo('Pausing torrents download')
                self.pause()
            else:
                rospy.logdebug('Not pausing, current state is [%s]'%self.state)
        except Exception as inst:
              ros_node.ParseException(inst)

    def is_running(self):
        return self.alarm.is_running
    
    def is_paused(self):
        return self.alarm.is_paused

class DownloaderFSM:
    def __init__(self, **kwargs):
        try:
            # The states
            self.states=['Start', 'Setup', 'Downloading', 'Paused', 'Unlock', 'Finished', 'Error']
            
            # And some transitions between states. We're lazy, so we'll leave out
            # the inverse phase transitions (freezing, condensation, etc.).
            self.transitions = [
                { 'trigger': 'configure',   'source': 'Start',          'dest': 'Setup' },
                { 'trigger': 'download',    'source': 'Setup',          'dest': 'Downloading' },
                { 'trigger': 'pause',       'source': 'Setup',          'dest': 'Paused' },
                { 'trigger': 'incorporate', 'source': 'Downloading',    'dest': 'Setup' },
                { 'trigger': 'done',        'source': 'Downloading',    'dest': 'Finished' },
                { 'trigger': 'pause',       'source': 'Downloading',    'dest': 'Paused' },
                { 'trigger': 'unpause',     'source': 'Paused',         'dest': 'Downloading' },
                { 'trigger': 'halted_add',  'source': 'Paused',         'dest': 'Setup' },
                { 'trigger': 'download',    'source': 'Unlock',         'dest': 'Downloading' },
                { 'trigger': 'remove_pause','source': 'Paused',         'dest': 'Unlock' },
                { 'trigger': 'restart',     'source': 'Finished',       'dest': 'Start' },
                
                ## Failure transitions
                { 'trigger': 'fail',        'source': 'Start',          'dest': 'Error' },
                { 'trigger': 'fail',        'source': 'Setup',          'dest': 'Error' },
                { 'trigger': 'fail',        'source': 'Downloading',    'dest': 'Error' },
                { 'trigger': 'fail',        'source': 'Paused',         'dest': 'Error' },
                { 'trigger': 'fail',        'source': 'Finished',       'dest': 'Error' },
                { 'trigger': 'reset',       'source': 'Error',          'dest': 'Start' }
            ]
            
            # Initialize
            self.fsm = TorrentDownloader(**kwargs)
            self.machine = Machine(self.fsm, 
                                   states=self.states, 
                                   transitions=self.transitions, 
                                   initial='Start')
            
            self.machine.on_enter_Unlock        ('unlocking')
            self.machine.on_enter_Setup         ('set_up_configuration')
            self.machine.on_enter_Downloading   ('start_downloading')
            self.machine.on_enter_Finished      ('close_torrent')
            self.machine.on_enter_Paused        ('pausing_download')
            self.machine.on_exit_Paused         ('resume')
            self.machine.on_exit_Error          ('reset_error')
            self.machine.on_enter_Error         ('failed_transition')
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def next(self, next_state, **args):
        try:
            ## Keeping previous state
            self.fsm.previous_state = self.fsm.state
            rospy.logdebug('Going to NEXT state, previous state: '+self.fsm.previous_state)
            
            ## Starting timed download alarm
            handles = self.fsm.ses.get_torrents()
            num_handles = len(handles)

            ## Rotating FSM
            if next_state == 'configure':
                self.fsm.configure(torrent=args['info'])
            elif next_state == 'incorporate':
                self.fsm.incorporate(torrent=args['info'])
            elif next_state == 'halted_add':
                self.fsm.halted_add(torrent=args['info'])
            elif next_state == 'download':
                self.fsm.download()
            elif next_state == 'pause':
                self.fsm.pause()
            elif next_state == 'unpause':
                
                ## There is nothing to download
                if num_handles < 1:
                    rospy.logdebug('Keeping paused, nothing to download')
                    return
                
                self.fsm.unpause()
            elif next_state == 'remove_pause':
                
                ## There is nothing to download
                if num_handles < 1:
                    rospy.logdebug('Keeping paused, nothing to download')
                    return
                
                self.fsm.remove_pause()
            elif next_state == 'done':
                self.fsm.done()
            else:
                rospy.logwarn('Unknown NEXT state')
            
            ## TODO: Check why args are not removed after looping...
            args = None
            
            rospy.loginfo('Current state: '+self.fsm.state)
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def close(self):
        try:
            rospy.loginfo('Doing nothing to close')
        except Exception as inst:
              ros_node.ParseException(inst)

    def make_magnet(self, msg, topic):
        ### torrent_info = []
        try:
            rospy.loginfo('Getting magnet to download')
            ## Get best magnet to download
            if 'found_torrents' not in topic:
                return msg.magnet
            
            ## Look for maximum seeds
            for i in range(len(msg['torrent_items'])):
                
                ## Initialising list with empty magnets
                item = msg['torrent_items'][i]
                
                ## Checking current torrent status
                if self.fsm.is_torrent_finished(item):
                    rospy.loginfo("[%s] has been already downloaded"%item['title_long'])
                    #pprint(item)
                    continue
                
                ## Looking into best match to download
                rospy.logdebug('Looking torrents of [%s]'%item['title_long'])
                max_seeds       = -1
                chosen_index    = -1
                for j in  range(len(item['torrents'])):
                    torrent     = item['torrents'][j]
                    
                    ## Get manget based on amount of seeds
                    if torrent['seeds'] > max_seeds:
                        max_seeds       = torrent['seeds']
                        chosen_index    = j
                        
                        ### torrent_info[i]['magnet']   = torrent.magnet
                        ### torrent_info[i]['id']       = item.id
                        ### torrent_info[i]['title']    = item.title_long
                        
                        rospy.logdebug('Looking for [%s] for %s-%s of %s with %d/%d'%
                                      (item['title_long'], torrent['type'], 
                                       torrent['quality'], torrent['size'],
                                       torrent['seeds'], torrent['peers']))
                
                ## Set selected torrent based in highest amount of seeds
                operation = 'selected'
                item['torrents'][chosen_index]['state']['status'] = operation
                item['torrents'][chosen_index]['state']['history'].append(set_history(operation))
                selected = item['torrents'][chosen_index]
                rospy.loginfo('Choosing [%s] for %s-%s of %s with %d/%d'%
                              (item['title_long'], selected['type'], 
                               selected['quality'], selected['size'],
                               selected['seeds'], selected['peers']))
                
                ## Should we update a selected state in this scope?
                self.fsm.update_db_state(selected['hash'], operation)
                
                ## TODO: Make rules to download
                ##        - Quality
                ##        - Does it has seeds/peers?
                ##        - Has it been already downloaded?
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return msg

    def set_download_pause(self, value):
        preivous = self.fsm.alarm.download_pause
        self.fsm.alarm.download_pause = value
        rospy.logdebug('  Setting download_pause from [%f] to [%f]'%(preivous, value))

    def set_download_time(self, value):
        preivous = self.fsm.alarm.download_time
        self.fsm.alarm.download_time = value
        rospy.logdebug('  Setting download_time from [%f] to [%f]'%(preivous, value))

class DownloadTorrent(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## Local variables
            self.downloader     = None
            self.download_time  = None
            self.download_pause = None
            self.args           = kwargs
            
            ## This variable has to be started before ROS
            ##   params are called
            self.lock       = threading.Lock()
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            
            ## Shutdown signal
            rospy.on_shutdown(self.ShutdownCallback)
            
            ## Initialising parent class with all ROS stuff
            super(DownloadTorrent, self).__init__(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)

    def Init(self):
        try:
            
            self.download_time = self.GetParam('/download_torrent/download_time')
            rospy.logdebug('+ Got download_time of [%s]'%str(self.download_time))
            self.download_pause = self.GetParam('/download_torrent/download_pause')
            rospy.logdebug('+ Got download_pause of [%s]'%str(self.download_pause))
            
            ## Local FSM
            self.args.update({
                'database':      'yts',
                'collection':    'torrents',
                'download_time':  self.download_time,
                'download_pause': self.download_pause
            })
            self.downloader = DownloaderFSM(**self.args)
            
            ## Executioning thread
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)

    def SubscribeCallback(self, msg, topic):
        try:

            if 'download_time' in topic:
                self.downloader.set_download_time(msg.data)
                return
            
            if 'download_pause' in topic:
                self.downloader.set_download_pause(msg.data)
                return
                
            ## Add item to queue
            self.queue.put((topic, msg))

            ## Notify data is in the queue
            with self.condition:
                self.condition.notifyAll()

        except Exception as inst:
              ros_node.ParseException(inst)

    def Run(self, event):
        ''' Run method '''
        try:
            rospy.loginfo('Running FSM')
            while not rospy.is_shutdown():
                ## Wait for being notified that a message
                ##    has arrived
                args = {}
                with self.condition:
                    rospy.logdebug('  Waiting for message in state -> [%s]'%
                                   self.downloader.fsm.state)
                    self.condition.wait()
                
                with self.lock:
                    (topic, msg) = self.queue.get()
                
                rospy.loginfo('Received query, current state -> [%s] -> [%s]'%
                              (self.downloader.fsm.previous_state, self.downloader.fsm.state))
                if 'move_state' in topic:
                    rospy.logdebug('Moving state on request')
                    self.downloader.next(msg.state, **args)
                    continue
                
                ## Checking if message is empty
                if len(msg.torrent_items) < 1:
                    rospy.loginfo("Empty query")
                    continue

                ## Get best magnet to download
                ## Making ROS message into a dictionary
                msg = json.loads(rj.convert_ros_message_to_json(msg) )
                msg = self.downloader.make_magnet(msg, topic)

                ## If it is already downloading to to setup
                next_state = 'configure'
                if self.downloader.fsm.is_running():
                    next_state = 'incorporate'
                elif self.downloader.fsm.is_paused():
                    next_state = 'halted_add'

                ## Going to next state with input data
                args = {'info': msg}
                rospy.logdebug('Setting up FSM state [%s]'%next_state)
                self.downloader.next(next_state, **args)

        except Exception as inst:
              ros_node.ParseException(inst)
          
    def ShutdownCallback(self):
        try:
            rospy.logdebug('+ Shutdown: Closing torrent down loader')
            self.downloader.close()
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

    setup_opts = OptionGroup(parser, "Downloading options")
    setup_opts.add_option('--save_path',
                type="string",
                action='store',
                default='/opt/data/movies',
                help='Path to downloaded items')
    setup_opts.add_option('--up_limit',
                type="int",
                action='store',
                default=50*1000,
                help='Upload limit (bytes)')
    setup_opts.add_option('--max_connections',
                type="int",
                action='store',
                default=-1,
                help='Maximum connections')
    
    parser.add_option_group(setup_opts)
    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('download_torrent', anonymous=False, log_level=logLevel)
    
    ## Sending logging to syslog
    if options.syslog:
        logging_utils.update_loggers()

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~move_state',                 State),
        ('~start',                      State),
        ('~set_download_pause',         Float64),
        ('~set_download_time',          Float64),
        ('/yts_finder/found_torrents',  YtsTorrents)
    ]
    pub_topics     = [
#         ('/event_locator/updated_events', WeeklyEvents)
    ]
    system_params  = [
        '/download_torrent/download_time',
        '/download_torrent/download_pause'
    ]
    
    ## Defining arguments
    args.update({'up_limit':        options.up_limit})
    args.update({'max_connections': options.max_connections})
    args.update({'save_path':       options.save_path})

    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = DownloadTorrent(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

