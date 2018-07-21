"""The Pirate Bay Module."""

import logging
import sys
import pprint
import datetime
import threading
import Queue
import requests
import time
import bs4
import copy
import re
import rospy

from hs_utils import ros_node 
from hs_utils.mongo_handler import MongoAccess
from bs4 import BeautifulSoup
from collections import Counter
from torrench.utilities.Config import Config

## Setting UTF-8 as default
reload(sys)
sys.setdefaultencoding('utf8')

## TODO: Make ascii input raw data, line: 112

class LimeTorrentsCrawler(Config):
    """
    LimeTorrents class.

    This class fetches torrents from LimeTorrents proxy,
    and diplays results in tabular form.

    All activities are logged and stored in a log file.
    In case of errors/unexpected output, refer logs.
    """

    def __init__(self, **kwargs):
        """Initialisations."""
        try:
            self.condition      = threading.Condition()
            Config.__init__(self)
            ## Initialising class variables
            self.class_name = self.__class__.__name__.lower()

            self.title      = None
            self.search_type= None
            self.with_magnet= None
            self.pages      = None
            self.db_handler = None
            self.soup_dict  = None
            self.updated_items = 0
            self.page_counter  = 0
            self.thread1_running = threading.Event()
            self.thread2_running = threading.Event()
            
            for key, value in kwargs.iteritems():
                if "title" == key:
                    self.title = value
                elif "page_limit" == key:
                    self.pages = value
                elif "search_type" == key:
                    self.search_type = value
                elif "with_magnet" == key:
                    self.with_magnet = value
                elif "db_handler" == key:
                    rospy.logdebug('   --> Defined DB handler')
                    self.db_handler = value
                elif "soup_dict" == key:
                    rospy.logdebug('   --> Defined shared queue')
                    self.soup_dict = value
                
            self.proxies    = self.get_proxies('limetorrents')
            self.proxy      = None
            self.total_fetch_time = 0
            self.waiting_time = 30

            self.missed     = Queue.Queue()
            self.missed_links = Queue.Queue()
            self.soup       = None
            self.headers    = ['NAME', 'INDEX', 'SIZE', 'SE/LE', 'UPLOADED']
            self.key1       = 'http://itorrents.org/torrent/'
            self.key2       = '.torrent?'
            
            self.lock       = threading.Lock()
            self.crawler_finished = False
            self.parser_finished = False
            
            ## Initialising processes for crawing and parsing 
            self.Init()    
        except Exception as inst:
          ros_node.ParseException(inst)

    def http_request(self, url):
        """
        This method does not calculate time.
        Only fetches URL and prepares self.soup
        """
        returned_code = None
        try:
            try:
                self.raw        = requests.get(url, timeout=30)
                returned_code   = self.raw.status_code
                rospy.logdebug("  +   Returned status code: %d for url %s" % (returned_code, url))
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                print(e)
                rospy.logwarn('HTTP request failed for: %s'%str(url))
                return None, returned_code
            if isinstance(self.raw, requests.models.Response):
                self.raw = self.raw.content
            soup = BeautifulSoup(self.raw, 'lxml')
            return soup
        except KeyboardInterrupt as e:
            print("Keyboard interrupted Aborted!")
            print(e)
        except Exception as inst:
          ros_node.ParseException(inst)
    
    def http_request_timed(self, url):
        """
        http_request_time method.

        Used to fetch 'url' page and prepare soup.
        It also gives the time taken to fetch url.
        """
        returned_code = None
        try:
            try:
                headers = {"user-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0"}
                self.start_time     = time.time()
                self.raw            = requests.get(url, timeout=15, headers=headers)
                returned_code       = self.raw.status_code
                self.page_fetch_time= time.time() - self.start_time
                rospy.logdebug("  +   Returned status code: %d for URL %s" % (returned_code, url))
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                rospy.logwarn('HTTP request failed for: %s'%str(url))
                return -1, self.page_fetch_time, returned_code
            except KeyboardInterrupt as e:
                print("Keyboard interrupted Aborted!")
            
            ## Getting response
            if isinstance(self.raw, requests.models.Response):
                self.raw            = self.raw.content
            soup                    = BeautifulSoup(self.raw, 'lxml')
            return soup, self.page_fetch_time, returned_code

        except KeyboardInterrupt as e:
            print("Keyboard interrupted Aborted!")

    def get_magnet_ext(self, link):
        """
        Module to get magnetic link of torrent.

        For 1337x, limetorrents modules.
        Magnetic link is fetched from torrent's info page.
        """
        try:
            magnet = None
            rospy.logdebug("T2:         Retrieving magnetic link with [%s]"%self.class_name)
            #soup = self.http_request(link)
            soup, time_, returned_code = self.http_request_timed(link)
            
            if returned_code != 200:
                rospy.logwarn("T2:         Returned code [%s] captured page [%s]"% (str(returned_code), link))
                return magnet

            coincidences_found = soup.findAll('div', class_='dltorrent')    
            if len(coincidences_found)<1:
                rospy.logwarn('T2:         Tag [dltorrent] not found')
                rospy.logdebug('T2:         Retrying magnetic link retrieving')
                coincidences_found = soup.findAll('div', class_='dltorrent')
                if len(coincidences_found)<1:
                    return magnet
                else:
                    rospy.logdebug('T2:         Not found dltorrent!!!')
            magnet = soup.findAll('div', class_='dltorrent')[2].a['href']
                    
        except Exception as inst:
          ros_node.ParseException(inst)
        finally:
            return magnet

    def check_proxy(self):
        """
        To check proxy availability.

        Proxy is checked in two steps:
        1. To see if proxy 'website' is available.
        2. A test is carried out with a sample string 'hello'.
        If results are found, test is passed, else test failed!

        This class inherits Config class. Config class inherits
        Common class. The Config class provides proxies list fetched
        from config file. The Common class consists of commonly used
        methods.

        In case of failiur, next proxy is tested with same procedure.
        This continues until working proxy is found.
        If no proxy is found, program exits.
        """
        count = 0
        result = False
        for proxy in self.proxies:
            rospy.logdebug("Trying %s" % (self.colorify("yellow", proxy)))
            rospy.logdebug("Trying proxy: %s" % (proxy))
            soup = self.http_request(proxy)
            try:
                ## print "=== What is self.soup?",self.soup
                if soup is not None and soup == -1:
                    rospy.logdebug("Bad proxy!")
                    count += 1
                    if count == len(self.proxies):
                        rospy.logdebug("No more proxies found! Terminating")
                        sys.exit(2)
                    else:
                        continue
                else:
                    rospy.logdebug("Proxy available. Performing test...")
                    url = proxy+"/search/all/hello/seeds/1/"
                    rospy.logdebug("Carrying out test for string 'hello'")
                    soup = self.http_request(url)
                    test = soup.find('table', class_='table2')
                    if test is not None:
                        self.proxy = proxy
                        rospy.logdebug("Pass!")
                        rospy.logdebug("Test passed!")
                        result = True
                        break
                    else:
                        rospy.logerr("Test failed! Possibly site not reachable. See logs.")
            except (AttributeError, Exception) as e:
                self.logger.exception(e)
                pass
            finally:
                return result
    
    def get_url(self, search_type, title=None):
        """
        Preparing type of query:
           1) Search of movie titles
           2) Browse existing movies
        """
        try:
            rospy.logdebug("  +   Defining a search type: "+search_type)
            if search_type == 'browse-movies':
                search_path = '/browse-torrents/Movies/seeds/'
            elif search_type == 'browse-shows':
                search_path = '/browse-torrents/TV-shows/seeds/'
            else:
                search_path = "/search/{}/{}/seeds/".format(search_type, title)
            rospy.logdebug("  +   Search link: "+search_path)
            return search_path
                
        except Exception as inst:
          ros_node.ParseException(inst)
            
    def get_html(self):
        """
        To get HTML page.

        Once proxy is found, the HTML page for
        corresponding search string is fetched.
        Also, the time taken to fetch that page is returned.
        Uses http_request_time() from Common.py module.
        """
        try:
            ## Populating queue of pages for HTML capture
            pages_queue = Queue.Queue()
            for page in range(self.pages):
                pages_queue.put(page)
            self.page_counter = 0

            while not pages_queue.empty():
                self.thread1_running.set()
                page = pages_queue.get()
                
                state = 'is running' if self.thread2_running.is_set() else 'has stopped'
                rospy.logwarn("T1:  Fetching page: %d/%s and PARSE_HTML %s" % 
                              (page+1, self.pages, state))
                rospy.logdebug("T1:  1.1) Fetching page: %d/%s" % (page+1, self.pages))
                
                search_title = self.get_url(self.search_type)
                search_title = search_title + '{}/'.format(page+1)
                search_url = self.proxy + search_title

                rospy.logdebug("T1:  1.2) Looking into:"+search_url)
                self.soup, time_, returned_code = self.http_request_timed(search_url)
                
                if str(type(self.soup)) == 'bs4.BeautifulSoup':
                    rospy.logerr("T1:Invalid HTML search type [%s]"%str(type(self.soup)))
                    rospy.loginfo("T1:       Waiting for [%d]s:"%self.waiting_time)
                    time.sleep(waiting_time)
                    pages_queue.put(page)
                    continue
                
                if returned_code != 200:
                    rospy.logwarn("T1:Returned code [%s] captured page [%s]"% (str(returned_code), search_url))
                    self.missed.put(search_url)
                    rospy.loginfo("T1:       Waiting for [%d]s:"%self.waiting_time)
                    with self.condition:
                        rospy.logdebug("T1:       Notifying html parsing with error code [%s]"%str(returned_code))
                        self.condition.notifyAll()
                    time.sleep(self.waiting_time)
                    pages_queue.put(page)
                    continue
                    
                rospy.loginfo("T1:  Captured page %d/%d in %.2f sec" % (page+1, self.pages, time_))
                self.total_fetch_time += time_
                    
                with self.lock:
                    rospy.logdebug("T1:  1.3) Placing page [%d] in queue"%page)
                    self.soup_dict.put({page : self.soup})
                    
                with self.condition:
                    if returned_code != 200:
                        rospy.logwarn('T1:Invalid HTTP code [%s], not notifying threads'%str(returned_code))
                    else:
                        rospy.logdebug("T1:  1.4) Notifying html parsing with error code [%s]"%str(returned_code))
                        self.condition.notifyAll()
                        self.page_counter += 1
            
            rospy.loginfo("T1:  + FINISHED: Got [%d] pages and finished with [%d] and missed [%d]"%
                              (self.page_counter, pages_queue.qsize(), self.missed.qsize()))

            self.crawler_finished = True
            
        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            ## Clearing thread flag
            self.thread1_running.clear()
    
    def parse_html(self):
        """
        Parse HTML to get required results.

        Results are fetched in masterlist list.
        Also, a mapper[] is used to map 'index'
        with torrent name, link and magnetic link
        """
        try:
            pages_parsed    = 0
            link_parsed     = 0
            while pages_parsed < 1 or self.soup_dict.qsize() >0:
                rospy.logdebug("-"*80)
                rospy.logdebug("T2:  2.1) Parsing [%s] torrent pages"%(self.soup_dict.qsize()))
                self.thread2_running.set()
                
                state = 'is running' if self.thread1_running.is_set() else 'has stopped'
                
                ## If there is nothing to do, wait for notification
                if self.soup_dict.empty():
                    rospy.logdebug("T2:  < A > Got notified but queue is empty")
                    
                    ## Waiting to be notified by other thread
                    with self.condition:
                        rospy.logdebug("T2:  < B > Waiting for HTML crawler notification...")
                        self.condition.wait()
                        rospy.logdebug("T2:  < C > Got notified  and GET_HTML %s" % (state))
                
                ## Acquiring lock for processing queue of retrieved pages
                with self.lock:
                    soupDict       = self.soup_dict.get()

                ## Getting HTML captured item
                soupKeys            = soupDict.keys()
                if len(soupKeys) <0:
                    rospy.logdebug("T2:  - No keys in queue, skiping URL parse")
                    continue

                ## Once soup has been collected, starting to parse
                page                = soupKeys[0]
                rospy.logdebug("T2:  2.2) Getting page [%d]"%(page+1))
                soup                = soupDict[page]

                ## Verifying soup is valid
                if not isinstance(soup, bs4.BeautifulSoup):
                    rospy.logerr("T2:Invalid HTML search item")

                ## Looking for table components
                content             = soup.find('table', class_='table2')
                rospy.logdebug("T2:  2.3) Looking for torrent data in page [%d]"%(page+1))
                if content is None:
                    rospy.logerr("T2:Invalid parsed content")
                    return

                results             = content.findAll('tr')
                for result in results[1:]:
                    ## Increasing links processed counter
                    link_parsed += 1

                    ## Getting source data
                    rospy.logdebug("    "+("  --"*5))
                    rospy.logdebug("T2:  2.4) Updating parsed web element")
                    data            = result.findAll('td')
                    
                    ## This method collects data from opened URL
                    ##     and updates content accordingly
                    element, dbItem = self.UpdateElement(data, pages_parsed)
                    if element is None:
                        rospy.logdebug("T2:  Element not updated")
                        return
                    
                    self.updated_items += 1
                    rospy.loginfo('T2:  2.6 [%s] items updated'%str(self.updated_items))

                ## Incremented torrents got magentic link
                pages_parsed += 1
                rospy.logdebug("T2: Remaining torrent pages [%s]"%(self.soup_dict.qsize()))
                if self.db_handler is not None:
                    rospy.logdebug("T2:  - Total records in DB: [%d]"%self.db_handler.Size())
                else:
                    rospy.logdebug("T2:  - Total parsed pages: [%d]"%pages_parsed)
                
                ## Retrying missing torrent tiles
                rospy.logwarn("T2:  + Found [%s] elements with missing link"%self.missed_links.qsize() )
                while self.missed_links.qsize()>0:
                    element         = self.missed_links.get()
                    link            = element['link']
                    rospy.logdebug("T2:  + Re-opening element [%s]"%element['hash'])
                    magnetic_link   = self.get_magnet_ext(link)
                    if magnetic_link is None:
                        rospy.logwarn('T2:  + No available magnet for: %s'%str(link))
                    else:
                        element.update({'magnetic_link': magnetic_link})
                        rospy.logdebug("T2:  + Appending in database [%s]"%element['hash'])
                        
                        ## BUG: This method updates only attributes leeches and seeds 
                        result = self.UpdateTimeSeries(element, dbItem, ['seeds', 'leeches'])
                        if not result:
                            rospy.logerr("T2:  + DB insertion failed")
                
                rospy.logdebug("T2:  2.5) Finished parsing HTML")
            
        except Exception as inst:
          ros_node.ParseException(inst)
        finally:
            ## Clearing thread flag
            self.thread2_running.clear()

    def Init(self):
        try:
            
            rospy.logdebug("Obtaining proxies...")
            proxy_ok = self.check_proxy()
            if proxy_ok:
                rospy.logdebug("Preparing threads")
            else:
                rospy.logdebug("Proxy failed")
                return

        except Exception as inst:
          ros_node.ParseException(inst)

    def UpdateBestSeriesValue(self, db_post, web_element, items_id):
        '''
        Comparator for time series update to check if for today already exists a value. 
        Could possible be if torrent hash is repeated in the website. 
        
        returns True if value has been updated. Otherwise, DB update failed
        '''
        
        result          = False
        try:
            hash        = db_post['hash']
            postKeys    = db_post.keys()
            
            ## Look for each time series item
            for key in items_id:
                
                ## Look for each time series element
                ##    of existing DB element
                if key in postKeys:
                    datetime_now        = datetime.datetime.utcnow()
                    month               = str(datetime_now.month)
                    month_exists        = month in db_post[key]['value'].keys()
                    
                    ## Checking if month exists
                    if not month_exists:
                        rospy.logdebug("T2:           Adding month [%s] to [%s]"%(month, key))
                        db_post[key]['value'].update({month : {}})
                        
                    ## Checking if day exists
                    day                 = str(datetime_now.day)
                    day_exist           = day in db_post[key]['value'][month].keys()
                    
                    ## If is value found in the website is bigger, use this one
                    ## otherwise let the one existing in the database
                    if day_exist:
                        todays_db       = db_post[key]['value'][month][day]
                        todays_website  = web_element[key]
                        isTodayBetter   = todays_db < todays_website
                        
                        ## TODO: We should know the page of both items
                        isTodayWorse    = todays_db >= todays_website
                        if isTodayWorse:
                            rospy.logdebug("T2:           Existing value for [%s] similar or better", key)
                        
                        # Updating condition and substitute values
                        elif isTodayBetter:
                            rospy.logdebug("T2:           Updating [%s] series item with hash [%s] in collection [%s]"% 
                                      (key, hash, self.db_handler.coll_name))
                        
                     
                    ## If day already exists check if it is better the one given 
                    ## right now by the website
                    set_key             = key+".value."+str(datetime_now.month)+"."+str(datetime_now.day)
                    subs_item_id        = {set_key: web_element[key] }
                    
                    ## if day is missing, add it!
                    condition           = { 'hash' : hash }
                    result              = self.db_handler.Update(condition, subs_item_id)
                    rospy.logdebug("T2:           Added [%s] series item for [%s/%s] with hash [%s] in collection [%s]"% 
                              (key, str(datetime_now.month), str(datetime_now.day), hash, self.db_handler.coll_name))

        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            return result

    def AddMissingKeys(self, db_post, web_element):
        '''
        Adds missing keys in existing DB records.
        '''
        result                  = True
        try: 
            if isinstance(db_post,type(None)):
                rospy.logdebug("T2: Invalid input DB post for printing")

            elementKeys         = web_element.keys()
            postKeys            = db_post.keys()
            postKeysCounter     = Counter(postKeys)
            elementKeysCounter  = Counter(elementKeys)
            missed_in_db        = (elementKeysCounter - postKeysCounter).keys()
           
#             extra_in_db         = (postKeysCounter - elementKeysCounter).keys()
#             for key in extra_in_db:
#                 if key != '_id':
#                     rospy.logdebug('T2:  -     TODO: Remove item [%s] from DB', key)
            
            if len(missed_in_db) > 0:
                for key in missed_in_db:
                    rospy.logdebug('T2: -     Updated item [%s] from DB', key)
                    result     = self.db_handler.Update(
                                    condition={"_id": db_post["_id"]}, 
                                    substitute={key: web_element[key]}, 
                                    upsertValue=False)
                    rospy.logdebug("T2:  -       Updated key [%s] in item [%s] of collection [%s]"% 
                                  (key, db_post["_id"], self.db_handler.coll_name))
        except Exception as inst:
            ros_node.ParseException(inst)
        finally:
            return result
    
    def GetWebData(self, data, page):
        element = None
        try:
            ## Getting hash
            torrent_file    = data[0].findAll('a')[0]['href']
            start_index     = torrent_file.find(self.key1)+len(self.key1)
            end_index       = torrent_file.find(self.key2)
            hash            = torrent_file[start_index:end_index]
            rospy.logdebug("T2:    2.4.1) Got hash [%s]"%(hash))
            
            # try block is limetorrents-specific. Means only limetorrents requires this.
            tag_found       = data[0].findAll('a')
#             print "===> tag_found:", tag_found
#             print "===> tag_found.len:", len(tag_found)
            link_index      = len(tag_found)-1
            name            = tag_found[link_index].string
            link            = tag_found[link_index]['href']
            link            = (self.proxy+link).encode('ascii', 'ignore').decode('ascii')
            date            = data[1].string
            date            = date.split('-')[0]
            size            = data[2].string
            seeds           = data[3].string.replace(',', '')
            leeches         = data[4].string.replace(',', '')
            rospy.logdebug("T2:    2.4.2) Parsed element data for [%s]"%str(hash))
            
            element         = {
                'name':         name,
                'date':         date,
                'size':         size,
                'seeds':        seeds,
                'leeches':      leeches,
                'link':         link,
                'page':         page+1,
                'hash':         hash,
                'torrent_file': torrent_file
            }

            ## Getting available images
            images          = data[0].findAll('img')
            qualifiers      = []
            if len(images) > 0:
                for image in images:
                    qualifiers.append(image['title'])
                element.update({'qualifiers': qualifiers})
        except Exception as inst:
          ros_node.ParseException(inst)
        finally:
            return element

    def UpdateElement(self, data, page):
        element = None
        try:
            tsItems         = ['seeds', 'leeches']
            element         = self.GetWebData(data, page)
            if element is None:
                rospy.logdebug("T2:    2.4.3) Built invalid element")
                return element
            
            ## Looking existing record
            hash            = element['hash']
            rospy.logdebug("T2:  2.5) Looking for existing record of [%s]"%hash)
            posts           = self.db_handler.Find({'hash': hash })
            postsSize       = posts.count()
            if postsSize < 1:
                rospy.logdebug("T2:    2.5.1) Element [%s] did not exist"%(hash))
                
                ## Update magnet
                element     = self.RetrieveMagnet(element)
                dbItem      = None
            else:
                if postsSize > 1:
                    rospy.logwarn("T2:    2.5.1) Element [%s] has multiple instances"%(hash))
                else:
                    rospy.logdebug("T2:    2.5.1) Element [%s] found"%(hash))
            
                ## Collecting existing post in DB
                dbItem      = posts[0]
                
                ## Check if DB element has a magnetic link
                if 'magnetic_link' not in dbItem.keys() or dbItem['magnetic_link'] == None:
                    rospy.logdebug("T2:    2.5.2) Getting magnet link")
                    element = self.RetrieveMagnet(element)
                else:
                    rospy.logdebug("T2:    2.5.2) DB record has magnetic link")
                    
                ## Checking if current element differs from DB record
                dbKeys      = dbItem.keys()
                elemKeys    = element.keys()
                hasChanged  = False
                for ekey in elemKeys:
                    ## If the element key does not exists in DB record
                    ##    then log the problem
                    updateDb            = False
                    if ekey not in dbKeys:
                        rospy.logwarn("T2:     2.5.3) Key [%s] do not exists in DB elements"%ekey)
                        updateDb        = True

                    ## If the element key is different to current DB 
                    ##    record, then use the element data
                    elif ekey not in tsItems and element[ekey] != dbItem[ekey]:
                        updateDb        = True
                    
                    if updateDb:
                        hasChanged      = True
                        dbItem[ekey]    = element[ekey]
                        updated_items   = {ekey: element[ekey] }
                        query_condition = { 'hash' : hash }
                        
                        rospy.logdebug("T2:       Updating key [%s] in DB elements"%ekey)
                        result          = self.db_handler.Update(query_condition, updated_items)
                
                if not hasChanged:
                    rospy.logdebug("T2:       Element [%s] has not been updated"%hash)
                
                ## Updating time series section
                result = self.UpdateTimeSeries(element, dbItem, tsItems)
                
            return element, dbItem
                        
        except Exception as inst:
          ros_node.ParseException(inst)

    def RetrieveMagnet(self, element):
        try:
            ## Getting magnet link
            hash            = element['hash']
            link            = element['link']
            magnetic_link   = self.get_magnet_ext(link)
            if magnetic_link is None:
                rospy.logwarn('T2:+  No available magnet for [%s] with link [%s]'%
                              (hash, str(link)))
                
                ## Adding element to missing links list
                self.missed_links.put(element)
                rospy.logwarn('T2:+  Added [%s] to missing links, size [%s]'%
                              (hash, str(self.missed_links.qsize())))
                rospy.loginfo("T2:+  Waiting for [%d]s:"%self.waiting_time)
                time.sleep(self.waiting_time)

                magnetic_link = ''
            rospy.logdebug("T2:+  Updating magnet link for [%s]"%(hash))
            element.update({'magnetic_link': magnetic_link})        
        except Exception as inst:
          ros_node.ParseException(inst)
        finally:
            return element

    def UpdateTimeSeries(self, element, dbItem, items_id):
        '''
        Generate a time series model 
        https://www.mongodb.com/blog/post/schema-design-for-time-series-data-in-mongodb
        '''
        result = False
        try:
            ## Get current date
            datetime_now    = datetime.datetime.utcnow()
            hash            = element['hash']
            rospy.logdebug("T2:    2.5.4) Updating time series section for [%s]"%hash)
            
            ## If element does not exists in DB,
            ##    Generate a new time series item
            if dbItem is None:
                ## Create time series model for time series
                def get_day_timeseries_model(value, datetime_now):
                    return { 
                        "timestamp_day": datetime_now.year,
                        "value": {
                            str(datetime_now.month): {
                                str(datetime_now.day) : value
                            }
                        }
                    };
                
                ## Generate time series schema for each
                ##    required key item (seeds, leeches)
                
                rospy.logdebug('T2:    2.5.4.1) Generating time series schema for [%s]', hash)
                for key_item in items_id:
                    element[key_item]   = get_day_timeseries_model(element[key_item], datetime_now)
                    
                ## Inserting time series model
                post_id             = self.db_handler.Insert(element)
                rospy.logdebug("T2:  -     Inserted time series item with hash [%s] in collection [%s]"% 
                                  (hash, self.db_handler.coll_name))
                result = post_id is not None
            else:
                ## 1) Check if items for HASH already exists
                ts_updated = self.UpdateBestSeriesValue(dbItem, element, items_id)
                if ts_updated:
                    rospy.logdebug('T2:    2.5.4.1) Time series updated for [%s]', hash)
                else:
                    rospy.logerr('T2:    2.5.4.1) DB Updated failed or time series not updated for [%s]', hash)
                result = ts_updated
            
        except Exception as inst:
            result = False
            ros_node.ParseException(inst)
        finally:
            return result
