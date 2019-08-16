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

from transitions import Machine
import libtorrent as lt
from __builtin__ import True
from future.backports.test.pystone import TRUE

def set_history(operation):
    try:
        return {'operation': operation, 'last_update': time.time()}
    except Exception as inst:
        ros_node.ParseException(inst)

class TorrentDownloader(object):
    def __init__(self, **kwargs):
        try:
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
            
            ## This variable has to be started before ROS
            ##   params are called
            self.fsm_condition  = threading.Condition()
            
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
            
            ## Looking for selected torrents
            for torrent_info in torrent['torrent_items']:
                rospy.logdebug('Setting up [%s] to download'%torrent_info['title_long'])
                for torrent_data in torrent_info['torrents']:
                    if torrent_data['state']['status'] == 'selected':

                        ## Preparing to download torrent
                        torernt_name = torrent_info['title_long']+'-'+torrent_data['quality']+'-'+torrent_data['type']
                        rospy.loginfo('  Preparing to download torrent [%s]'%(torernt_name))
                        handle = lt.add_magnet_uri(self.ses, torrent_data['magnet'], self.params) 
                        
                        rospy.loginfo('     Downloading metadata...')
                        start_time = time.time()
                        metadata_ready = True
                        metadata_expired = True
                        while (not handle.has_metadata() and self.close_now and metadata_expired): 
                            time.sleep(0.5)
                            metadata_expired = (time.time() - start_time) <= 60
                            #print "===> metadata_expired:", metadata_expired, self.handle.has_metadata()
                            
                        if not handle.has_metadata():
                            rospy.logwarn('  Meta data not available for [%s]'%torernt_name)
                        else:
                            rospy.loginfo('  Got metadata for [%s], starting torrent download...'%torernt_name)
                        
                        ## Setting up connection options
                        handle.set_upload_limit(self.up_limit)
                        handle.set_max_connections(self.max_connections)
                        
            ## Moving to next state
            self.download(torrent_info=torrent['torrent_items'])
            
        except Exception as inst:
            ros_node.ParseException(inst)
            self.fail()
              
    def start_downloading(self, torrent_info=None): 
        try:
            if torrent_info is None:
                rospy.logwarn ('Downloading torrent failed as no torrent info was provided')
                ## TODO: Send it to error state?
                return
            
            if not self.is_Downloading():
                self.failed_downloading()
            else:
                ## Verify is session isn't already downloading stuff...
                print "===> session is_listening?", self.ses.is_listening()
                if not self.download_started:
                    self.download_started = True
                    rospy.logdebug('---> Starting to download ['+self.previous_state+'] -> ['+self.state+']')
                    
                    ## Update new state in DB and locally
                    rospy.logdebug('  Updating downloading status for all handlers')
                    handles = self.ses.get_torrents()
                    for handle in handles:
                        status = handle.status()
                        hash = str(status.info_hash).upper()
                        self.update_db_state(hash, 'downloading')
                
                    ## Downloading thread
                    rospy.Timer(rospy.Duration(0.5), self.downloader_thread, oneshot=True)
                else:
                    rospy.logdebug('---> Download thread already started  ['+self.previous_state+'] -> ['+self.state+']')
             
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
              
    def restore_download(self): 
        try:
            if not self.is_Paused():
                self.failed_pause()
            else:
                rospy.logdebug('---> Restore download ['+self.previous_state+'] -> ['+self.state+']')
                
            ## Notify data is in the queue
            with self.fsm_condition:
                self.fsm_condition.notifyAll()
            
            ## Resume session
            self.ses.resume()
        except Exception as inst:
              ros_node.ParseException(inst)

    def failed_downloading(self): 
        try:
            rospy.logwarn('---> Failed downloading...')
        except Exception as inst:
              ros_node.ParseException(inst)

    def reset_download(self): 
        try:
            rospy.logdebug('---> Resetting download ['+self.previous_state+'] -> ['+self.state+']')
        except Exception as inst:
              ros_node.ParseException(inst)

    def downloader_thread(self, event):
        ''' Run method '''
        try:
            while (not self.status.is_seeding and self.not_complete):
                if self.state == 'Paused':
                    with self.fsm_condition:
                        rospy.loginfo('  Download has been halted')
                        self.fsm_condition.wait()

                self.status = self.handle.status()
                
                #rospy.logdebug('['+self.previous_state+'] -> ['+self.state+']')
                progress        = self.status.progress * 100.0
                download_rate   = self.status.download_rate / 1000
                upload_rate     = self.status.upload_rate / 1000
                num_peers       = self.status.num_peers
                state           = self.state_str[self.status.state]
                previous_state  = self.previous_state
                current_state   = self.state
                
                rospy.loginfo(' %.2f%% complete (down: %.1f kb/s, up: %.1f kB/s, peers: %d) %s: [%s] -> [%s]'% (
                    progress, 
                    download_rate, 
                    upload_rate, 
                    num_peers, 
                    state,
                    previous_state,
                    current_state
                ))
                
                if not self.close_now:
                    print "===> CLOSING"
                    break
            
                if self.status.progress  >= 1.0:
                    self.not_complete = False
                    print "===> FALSE"
                else:
                    time.sleep(1)
            
#                     print "===> not_complete:", self.not_complete
#                     print "===> status.is_seeding:", self.status.is_seeding
#                     print "===> status:", self.status_seeding

            ## Moving to next state
            print "Download finished"
            self.done()
        except Exception as inst:
              ros_node.ParseException(inst)

    def update_db_state(self, torrent_info):
        try:
            ## Accessing DB elemnt with ID
            for torrent in torrent_info:
                target_state = 'downloading'
                rospy.logdebug("  Updating to [%s] state for %d/%s"%
                               (target_state, torrent['id'], torrent['title']))
                query = {'id': torrent['id']}
                posts = self.db_handler.Find(query)
                for element in posts:
                    pprint(element)
                    print "-"*10
                    
                ## Updating current torrent history and state
                # 'hs_state': {u'history': [], u'status': u'created'}
                    substitute = element['hs_state']
                    substitute['status'] = target_state
                    substitute['history'].append({
                        'last_update': rospy.Time.now().to_sec(),
                        'operation': ,
                    })
                    pprint(substitute)
                    #updated_was_ok = self.db_handler.Update(query, substitute)
        except Exception as inst:
              ros_node.ParseException(inst)
        
class DownloaderFSM:
    def __init__(self, **kwargs):
        try:
            # The states
            self.states=['Start', 'Setup', 'Downloading', 'Paused', 'Finished', 'Error']
            
            # And some transitions between states. We're lazy, so we'll leave out
            # the inverse phase transitions (freezing, condensation, etc.).
            self.transitions = [
                { 'trigger': 'configure',           'source': 'Start',          'dest': 'Setup' },
                { 'trigger': 'download',            'source': 'Setup',          'dest': 'Downloading' },
                { 'trigger': 'done',                'source': 'Downloading',    'dest': 'Finished' },
                { 'trigger': 'pause',               'source': 'Downloading',    'dest': 'Paused' },
                { 'trigger': 'unpause',             'source': 'Paused',         'dest': 'Downloading' },
                { 'trigger': 'reset',               'source': 'Finished',       'dest': 'Start' },
                { 'trigger': 'fail_setup',          'source': 'Setup',          'dest': 'Error' },
                { 'trigger': 'failed_downloading',  'source': 'Downloading',    'dest': 'Error' },
                { 'trigger': 'failed_pause',        'source': 'Paused',         'dest': 'Error' },
                { 'trigger': 'failed_closing',      'source': 'Finished',       'dest': 'Error' }
            ]
            
            # Initialize
            self.fsm = TorrentDownloader(**kwargs)
            self.machine = Machine(self.fsm, 
                                   states=self.states, 
                                   transitions=self.transitions, 
                                   initial='Start')
            
            self.machine.on_enter_Setup('set_up_configuration')
            self.machine.on_enter_Downloading('start_downloading')
            self.machine.on_enter_Finished('close_torrent')
            self.machine.on_enter_Paused('pausing_download')
            self.machine.on_exit_Paused('restore_download')
            self.machine.on_exit_Paused('reset_downloader')
            
        except Exception as inst:
              ros_node.ParseException(inst)

    def next(self, next_state, **args):
        try:
            #print "===> next_state", next_state
            #print "===> args", args
            ## Keeping previous state
            self.fsm.previous_state = self.fsm.state
            rospy.logdebug('Previous state: '+self.fsm.previous_state)
            
            ## Rotating FSM
            if next_state == 'configure':
                
                if "torrent_info" not in args.keys() :
                    rospy.logwarn('No torrent info in state transition')
                    return
                self.fsm.configure(torrent_info=args['torrent_info'])
            elif next_state == 'download':
                self.fsm.download()
            elif next_state == 'pause':
                self.fsm.pause()
            elif next_state == 'unpause':
                self.fsm.unpause()
            elif next_state == 'done':
                self.fsm.done()
            
            rospy.loginfo('Current state: '+self.fsm.state)
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def close(self):
        try:
            self.fsm.close_now = False
            print "-", self.fsm.close_now
        except Exception as inst:
              ros_node.ParseException(inst)

    def choose_magnet(self, msg, topic):
        torrent_info = []
        try:
            rospy.loginfo('Getting magnet to download')
            ## Get best magnet to download
            if 'found_torrents' not in topic:
                return msg.magnet
            
            #pprint(msg)
            torrent_template = {
                'magnet' : '',
                'id': '',
                'title': ''
            }
            for i in range(len(msg.torrent_items)):
                
                ## Initialising list with empty magnets
                item = msg.torrent_items[i]
                torrent_info.append( copy.copy(torrent_template) )
                
                ## Looking into best match to download
                rospy.logdebug('Looking torrents of [%s]'%item.title_long)
                max_seeds = -1
                for torrent in item.torrents:
                    
                    ## Get manget based on amount of seeds
                    if torrent.seeds > max_seeds:
                        max_seeds = torrent.seeds
                        torrent_info[i]['magnet']   = torrent.magnet
                        torrent_info[i]['id']       = item.id
                        torrent_info[i]['title']    = item.title_long
                        
                        rospy.loginfo('Getting [%s] for %s-%s of %s with %d/%d'%
                                      (item.title_long, torrent.type, 
                                       torrent.quality, torrent.size,
                                       torrent.seeds, torrent.peers))
                        
                    ## TODO: Make rules to download
                    ##        - Quality
                    ##        - Does it has seeds/peers?
                    ##        - Has it been already downloaded?
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return torrent_info
            
class DownloadTorrent(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## Local variables
            self.downloader     = None
            
            ## This variable has to be started before ROS
            ##   params are called
            self.lock       = threading.Lock()
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            
            ## Shutdown signal
            rospy.on_shutdown(self.ShutdownCallback)
            
            ## Initialising parent class with all ROS stuff
            super(DownloadTorrent, self).__init__(**kwargs)
            
            ## Local FSM
            kwargs.update({
                'database':     'yts',
                'collection':   'torrents'
            })
            self.downloader = DownloaderFSM(**kwargs)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)

    def Init(self):
        try:
            
            ## Executioning thread
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)

    def SubscribeCallback(self, msg, topic):
        try:

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
            rospy.logdebug('Running FSM')
            while not rospy.is_shutdown():
                ## Wait for being notified that a message
                ##    has arrived
                with self.condition:
                    rospy.logdebug('  Waiting for message...')
                    self.condition.wait()
                
                with self.lock:
                    (topic, msg) = self.queue.get()
                    
                args = {}
                if 'move_state' in topic:
                    self.downloader.next(msg.state, **args)
                
                else:
                    
                    ## Get best magnet to download
                    torrent_info = self.downloader.choose_magnet(msg, topic)
                    if len(torrent_info) < 1 or torrent_info is None:
                        rospy.logwarn('Invalid torrent data provided')
                        return
                    
                    ## Preparing state transition with input data
                    args = {
                        'torrent_info': torrent_info
                    }
                    
                    ## Going to next state
                    self.downloader.next('configure', **args)

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
                default='/opt/data/tmp',
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
        ('/yts_finder/found_torrents',  YtsTorrents)
    ]
    pub_topics     = [
#         ('/event_locator/updated_events', WeeklyEvents)
    ]
    system_params  = [
        #'/event_locator_param'
    ]
    
    ## Defining arguments
    args.update({'up_limit':        options.up_limit})
    args.update({'max_connections': options.max_connections})
    args.update({'save_path':       options.save_path})

    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = DownloadTorrent(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

