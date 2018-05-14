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
#             self.complete_cond  = threading.Condition()
            Config.__init__(self)
            ## Initialising class variables
            self.class_name = self.__class__.__name__.lower()

            self.title      = None
            self.search_type= None
            self.with_magnet= None
            self.pages      = None
            self.db_handler = None
            self.soup_dict  = None
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
            self.index      = 0
            self.page       = 0
            self.total_fetch_time = 0
            self.mylist     = []
            self.masterlist = []
            self.mylist_crossite = []
            self.masterlist_crossite = []
            self.mapper     = []
            ## self.soup_dict  = {}
            #self.soup_dict  = Queue.Queue()
            self.missed     = Queue.Queue()
            self.missed_links = Queue.Queue()
            self.soup       = None
            self.headers    = ['NAME', 'INDEX', 'SIZE', 'SE/LE', 'UPLOADED']
            ## self.key1       = 'magnet:?xt=urn:btih:'
            ## self.key2       = '&'
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
            rospy.logdebug("Fetching magnetic link with [%s]"%self.class_name)
            soup = self.http_request(link)
            if soup is None:
                rospy.logwarn('HTTP Request failed for: %s'%str(link))
                return magnet
            else:
                coincidences_found = soup.findAll('div', class_='dltorrent')    
                if len(coincidences_found)<1:
                    rospy.logwarn('  Tag [dltorrent] not found')
                    rospy.logdebug('  Retrying to get magnetic link')
                    coincidences_found = soup.findAll('div', class_='dltorrent')
                    if len(coincidences_found)<1:
                        return magnet
                    else:
                        rospy.logdebug('  Found dltorrent!!!')
                magnet = soup.findAll('div', class_='dltorrent')[2].a['href']
                    
        except Exception as inst:
          ros_node.ParseException(inst)
        finally:
            return magnet

    def get_magnet_download(self, link):
        """
        Module to get magnetic link of torrent.

        For 1337x, limetorrents modules.
        Magnetic link is fetched from torrent's info page.
        """
        rospy.logdebug("Fetching magnetic link with [%s]"%self.class_name)
        soup = self.http_request(link)
        magnet = soup.findAll('div', class_='dltorrent')[2].a['href']
        download_link = soup.findAll('div', class_='dltorrent')[0].a['href']
        return magnet, download_link
    
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
            self.soup = self.http_request(proxy)
            try:
                ## print "=== What is self.soup?",self.soup
                if self.soup is not None and self.soup == -1 or 'limetorrents' not in self.soup.find('div', id='logo').a['title'].lower():
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
                    self.soup = self.http_request(url)
                    test = self.soup.find('table', class_='table2')
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
            
    def get_html(self, cond=None):
        """
        To get HTML page.

        Once proxy is found, the HTML page for
        corresponding search string is fetched.
        Also, the time taken to fetch that page is returned.
        Uses http_request_time() from Common.py module.
        """
        try:
            
            self.crawler_finished = False
            
            ## Populating queue of pages for HTML capture
            pages_queue = Queue.Queue()
            for page in range(self.pages):
                pages_queue.put(page)
            page_counter = 0
            
            while not pages_queue.empty():
                page = pages_queue.get()
                
                state = 'is running' if self.thread2_running else 'has stopped'
                rospy.logwarn("T1:  Fetching page: %d/%s and PARSE_HTML %s" % 
                              (page+1, self.pages, state))
                rospy.logdebug("T1:  1.1) Fetching page: %d/%s" % (page+1, self.pages))
                
                search_title = self.get_url(self.search_type)
                search_title = search_title + '{}/'.format(page+1)
#                 print "===> self.proxy:", self.proxy
#                 print "===> search_title:", search_title
                search_url = self.proxy + search_title

                rospy.logdebug("T1:  1.2) Looking into:"+search_url)
                self.soup, time_, returned_code = self.http_request_timed(search_url)
                
#                 print "===> time:", time
#                 print "===> returned_code:", returned_code
#                 print "---> soup:", self.soup == None 
                
                if str(type(self.soup)) == 'bs4.BeautifulSoup':
                    rospy.logerr("T1:Invalid HTML search type [%s]"%str(type(self.soup)))
                    waiting_time = 30 
                    rospy.loginfo("T1:       Waiting for [%d]s:"%waiting_time)
                    time.sleep(waiting_time)
                    pages_queue.put(page)
                    continue
                
                if returned_code != 200:
                    rospy.logwarn("T1:Returned code [%s] captured page [%s]"% (str(returned_code), search_url))
                    self.missed.put(search_url)
                    waiting_time = 30
                    rospy.loginfo("T1:       Waiting for [%d]s:"%waiting_time)
                    with self.condition:
                        rospy.logdebug("T1:       Notifying html parsing with error code [%s]"%str(returned_code))
                        self.condition.notifyAll()
                    time.sleep(waiting_time)
                    pages_queue.put(page)
                    continue
                    
                rospy.loginfo("T1:  Captured page %d/%d in %.2f sec" % (page+1, self.pages, time_))
                self.total_fetch_time += time_
                    
                #with self.lock:
                rospy.logdebug("T1:  1.3) Placing page [%d] in queue"%page)
                self.soup_dict.put({page : self.soup})
                    
                with self.condition:
                    if returned_code != 200:
                        rospy.logwarn('T1:Invalid HTTP code [%s], not notifying threads'%str(returned_code))
                    else:
                        rospy.logdebug("T1:  1.4) Notifying html parsing with error code [%s]"%str(returned_code))
                        self.condition.notifyAll()
                        page_counter += 1
            
            rospy.loginfo("T1:  + Got [%d] pages and finished with [%d] and missing [%d]"%
                              (page_counter, pages_queue.qsize(), self.missed.qsize()))
            self.crawler_finished = True
            
            ## Call complete DB process
            rospy.logdebug('T1: Calling DB completion')
            self.run_complete()
        except Exception as inst:
            self.thread1_running = False
            ros_node.ParseException(inst)

    def parse_html(self, cond=None):
        """
        Parse HTML to get required results.

        Results are fetched in masterlist list.
        Also, a mapper[] is used to map 'index'
        with torrent name, link and magnetic link
        """
        try:
            self.parser_finished = False
            pages_parsed = 0
            while not self.crawler_finished:
                ## Waiting to be notified by other thread
                with self.condition:
                    rospy.logdebug("T2:  2.1) Waiting for HTML crawler notification...")
                    self.condition.wait()
                
                state = 'is running' if self.thread1_running else 'has stopped'
                rospy.logwarn("T2:  Got notified  and GET_HTML %s" % (state))
                ## Acquiring lock and collecting soup
                with self.lock:
                    if self.soup_dict.empty():
                        rospy.logdebug("T2:  - Got notified but queue is empty")
                        continue
                    soupDict       = self.soup_dict.get()

                ## Getting HTML captured item
                soupKeys            = soupDict.keys()
                if len(soupKeys) <0:
                    self.logger.debug("T2:  - No keys in queue, skiping URL parse")
                    continue

                ## Once soup has been collected, starting to parse
                page                = soupKeys[0]
                rospy.logdebug("T2:  2.2) Getting page [%d]"%(page+1))
                self.soup           = soupDict[page]
                
                ## Verifying soup is valid
                if not isinstance(self.soup, bs4.BeautifulSoup):
                    rospy.logerr("T2:Invalid HTML search item")
                    
                ## Looking for table components
                content             = self.soup.find('table', class_='table2')
                rospy.logdebug("T2:  2.3) Looking for data in page [%d]"%(page+1))
                
                if content is None:
                    rospy.logerr("T2:Invalid parsed content")
                    return
                results             = content.findAll('tr')
                for result in results[1:]:
                    data            = result.findAll('td')
                    # try block is limetorrents-specific. Means only limetorrents requires this.
                    tag_found       = data[0].findAll('a')
                    print "===> tag_found.len:", len(tag_found)
                    name            = tag_found[1].string
                    link            = tag_found[1]['href']
                    link            = self.proxy+link
                    date            = data[1].string
                    date            = date.split('-')[0]
                    size            = data[2].string
                    seeds           = data[3].string.replace(',', '')
                    leeches         = data[4].string.replace(',', '')
                    seeds_color     = self.colorify("green", seeds)
                    leeches_color   = self.colorify("red", leeches)
                    
                    self.index      += 1
                    self.mapper.insert(self.index, (name, link, self.class_name))
                    self.mylist     = [name, "--" +
                                    str(self.index) + "--", size, seeds_color+'/'+
                                    leeches_color, date]
                    self.masterlist.append(self.mylist)
                    self.mylist_crossite = [name, self.index, size, seeds+'/'+leeches, date]
                    self.masterlist_crossite.append(self.mylist_crossite)
                    
                    element         = {
                        'name': name,
                        'date': date,
                        'size': size,
                        'seeds': seeds,
                        'leeches': leeches,
                        'link': link,
                        'page': page+1
                    }

                    ## Getting hash
                    torrent_file    = data[0].findAll('a')[0]['href']
                    start_index     = torrent_file.find(self.key1)+len(self.key1)
                    end_index       = torrent_file.find(self.key2)
                    hash            = torrent_file[start_index:end_index]
                    element.update({'hash': hash})
                    element.update({'torrent_file': torrent_file})
                    
                    ## Getting available images
                    images          = data[0].findAll('img')
                    qualifiers      = []
                    if len(images) > 0:
                        for image in images:
                            qualifiers.append(image['title'])
                        element.update({'qualifiers': qualifiers})

                    ## Getting magnet link
                    if self.with_magnet:
                        rospy.logdebug("T2:      Getting magnet link")
                        magnetic_link = self.get_magnet_ext(link)
                        if magnetic_link is None:
                            rospy.logwarn('T2:No available magnet for: %s'%str(link))
                            magnetic_link = ''
                        element.update({'magnetic_link': magnetic_link})

                    ## Inserting in database
                    if self.with_db:
                        rospy.logdebug("T2:  2.4) Appending in database [%s]"%element['hash'])
                        result = self.Update_TimeSeries_Day(element, 
                                                    'hash',         ## This is the item key, it should be unique!
                                                    ['seeds', 'leeches'],  ## These items are defined in a time series
                                                    ) 
                        if not result:
                            rospy.logerr("T2:DB insertion failed")
                        
                    pages_parsed += 1
                    
                if self.with_db:
                    rospy.logdebug("T2:  - Total records in DB: [%d]"%self.db_handler.Size())
                else:
                    rospy.logdebug("T2:  - Total parsed pages: [%d]"%pages_parsed)
                    
                rospy.logdebug("T2:  2.5) Finished parsing HTML")
            
            self.parser_finished = True
            rospy.logdebug('T2: Found [%d] items'%pages_parsed)

            ## Call complete DB process
            rospy.logdebug('T2: Calling DB completion')
            self.run_complete()
        except Exception as inst:
          self.thread2_running = False
          ros_node.ParseException(inst)

    def complete_db(self, cond=None):
        '''
        '''
        try:
            with self.complete_cond:
                rospy.loginfo("T3:  3.1) Waiting for HTML crawler notification...")
                self.complete_cond.wait()
                
                items           = ['leeches', 'seeds']
                datetime_now    = datetime.datetime.utcnow()
                
                month           = str(datetime_now.month)
                day             = str(datetime_now.day)
                page_counter    = 0
                total_fetch_time= 0.0
                    
                rospy.logdebug('T3:    3.1) Searching non updated items')
                leeches_key     = 'leeches.value.'+month+"."+day
                seeds_key       = 'seeds.value.'+month+"."+day
                condition       = { '$or': [{ leeches_key : {'$exists': False}}, { seeds_key : {'$exists': False}}]}
                posts           = self.db_handler.Find(condition)
                postsSize       = posts.count()
                rospy.logdebug('T3:         Found [%d] items'%postsSize)
                
                rospy.logdebug('T3:    3.2) Creating queue of posts')
                posts_queue     = Queue.Queue()
                for post in posts:
                    posts_queue.put(post)
                
                while not posts_queue.empty():
                    post = posts_queue.get()
                    ##pprint.pprint(post)
                    search_url  = post['link'].encode("utf-8")
#                     print "===> search_url:", search_url
                    rospy.logdebug("T3:      3.2.1) Looking into URL")
                    
                    self.soup, time_, returned_code = self.http_request_timed(search_url)
            
                    if str(type(self.soup)) == 'bs4.BeautifulSoup':
                        rospy.logwarn("T3: Invalid HTML search type [%s]"%str(type(self.soup)))
                        waiting_time = 30 
                        rospy.loginfo("T3:       Waiting for [%d]s:"%waiting_time)
                        time.sleep(waiting_time)
                        posts_queue.put(post)
                        continue
                    
                    if returned_code != 200:
                        rospy.logwarn("T3: Returned code [%s] captured page [%s]"% (str(returned_code), search_url))
                        self.missed.put(search_url)
                        waiting_time = 30 
                        rospy.loginfo("T3:       Waiting for [%d]s:"%waiting_time)
                        time.sleep(waiting_time)
                        posts_queue.put(post)
                        continue
                        
                    if self.soup is None:
                        rospy.logwarn("T3: Invalid page retrieved")
                        continue

                    page_counter += 1
                    rospy.loginfo("T3:       Captured page %d/%d in %.2f sec" % (page_counter+1, postsSize, time_))
                    total_fetch_time += time_
                    
                    ## Looking for table components
                    rospy.logdebug("T3:      3.2.2) Searching for leechers, seeders and hash ID")
                    search_table    = self.soup.find('table')
                    lines           = search_table.findAll('td')
                    hash            = lines[2].string.strip()
                    
                    seeders_num     = None
                    seeders_data    = self.soup.body.findAll(text=re.compile('^Seeders'))
                    if len(seeders_data)>1:
                        seeders_num = seeders_data[0].split(':')[1].strip()
                        
                    leechers_num    = None
                    leechers_data   = self.soup.body.findAll(text=re.compile('^Leechers'))
                    if len(leechers_data)>1:
                        leechers_num= leechers_data[0].split(':')[1].strip()
                    
                    if post['hash'] == hash:
                    	rospy.logdebug("T3:      3.2.3) Updating item [%s] with [%s] leeches " % (hash, leechers_num))
                        condition   = { 'hash' : hash }
                        leeches_item= {leeches_key: leechers_num }
                        result      = self.db_handler.Update(condition, leeches_item)

                        rospy.logdebug("T3:      3.2.4) Updating item [%s] with [%s] seeds " % (hash, seeders_num))
                        condition   = { 'hash' : hash }
                        seeds_item  = {seeds_key: seeders_num }
                        result      = self.db_handler.Update(condition, seeds_item)

        except Exception as inst:
          ros_node.ParseException(inst)

    def run(self):
        try:
            
            rospy.logdebug("Obtaining proxies...")
            proxy_ok = self.check_proxy()
            if proxy_ok:
                rospy.logdebug("Preparing threads")
            else:
                rospy.logdebug("Proxy failed")
                return
                
            ## Generating one shot threads
            rospy.logdebug("Generating one shot threads")
            rospy.Timer(rospy.Duration(0.5), self.get_html,   oneshot=True)
            rospy.Timer(rospy.Duration(0.5), self.parse_html, oneshot=True)
            rospy.Timer(rospy.Duration(0.5), self.complete_db, oneshot=True)
        except Exception as inst:
          ros_node.ParseException(inst)
        
    def Run(self):
        '''
        Looks for all browse movies and parses HTML in two threads. 
        Then, updates any non feature torrent in DB 
        '''
        try:
            rospy.logdebug("Obtaining proxies...")
            proxy_ok = self.check_proxy()
            if proxy_ok:
                rospy.logdebug("Preparing threads")
                condition = threading.Condition()
                html_crawler = threading.Thread(
                    name='html_crawler', 
                    target=self.get_html, 
                    args=(condition,))
                html_parser  = threading.Thread(
                    name='html_parser',  
                    target=self.parse_html, 
                    args=(condition,))
                
                html_parser.start()
                html_crawler.start()
                
                html_crawler.join()
                html_parser.join()
                
                crawler_db  = threading.Thread(
                    name='crawler_db',  
                    target=self.complete_db, 
                    args=(condition,))
                crawler_db.start()
                crawler_db.join()

        except Exception as inst:
          ros_node.ParseException(inst)

    def run_complete(self):
        if self.crawler_finished and self.parser_finished:
            with self.complete_cond:
                self.complete_cond.notifyAll()
        else:
            crawler_result = 'is finished' if self.crawler_finished else 'has NOT finished'
            parser_result = 'is finished' if self.crawler_finished else 'has NOT finished'
            rospy.logdebug('Not started DB completion because, crawler %s and parser %s'%
                           (crawler_result, parser_result))

    def UpdateBestSeriesValue(self, db_post, web_element, item_index, items_id):
        '''
        Comparator for time series update to check if for today already exists a value. 
        Could possible be if torrent hash is repeated in the website. 
        
        returns True if value has been updated. Otherwise, DB update failed
        '''
        result      = True
        try:
            postKeys = db_post.keys()
            for key in items_id:
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
                     
                    ## If day already exists check if it is better the one given 
                    ## right now by the webiste
                    
                    condition           = { item_index : web_element[item_index] }
                    set_key             = key+".value."+str(datetime_now.month)+"."+str(datetime_now.day)
                    subs_item_id        = {set_key: web_element[key] }
                    if day_exist:
                        ## If is value found in the website is bigger, use this one
                        ## otherwise let the one existing in the database
                        todays_db       = db_post[key]['value'][month][day]
                        todays_website  = web_element[key]
                        isTodayBetter   = todays_db < todays_website
                        
                        ## TODO: We should know the page of both items
                        #print "=== [",datetime_now.month, "/", datetime_now.day,"], todays_db >= todays_website:", todays_db, '>=', todays_website
                        isTodayWorse    = todays_db >= todays_website
                        if isTodayWorse:
                            rospy.logdebug("T2:           Existing value for [%s] similar or better", key)
                        
                        # Updating condition and substitute values
                        elif isTodayBetter:
                            ## result = True
                            result      = self.db_handler.Update(condition, subs_item_id)
                            rospy.logdebug("T2:           Updated [%s] series item with hash [%s] in collection [%s]"% 
                                      (key, web_element[item_index], self.db_handler.coll_name))
                        
                    ## if day is missing, add it!
                    else:
                        ## result = True
                        result          = self.db_handler.Update(condition, subs_item_id)
                        rospy.logdebug("T2:           Added [%s] series item for [%s/%s] with hash [%s] in collection [%s]"% 
                                  (key, str(datetime_now.month), str(datetime_now.day), web_element[item_index], self.db_handler.coll_name))
                        
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
            extra_in_db         = (postKeysCounter - elementKeysCounter).keys()
            missed_in_db        = (elementKeysCounter - postKeysCounter).keys()
           
            for key in extra_in_db:
                if key != '_id':
                    rospy.logdebug('T2:  -     TODO: Remove item [%s] from DB', key)
            
            if len(missed_in_db) > 0:
                for key in missed_in_db:
                    rospy.logdebug('T2: -     Updated item [%s] from DB', key)
#                     print "===>_id: ", db_post["_id"]
#                     print "===>key:", key 
#                     print "===>web_element:", web_element[key] 
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
    
    def Update_TimeSeries_Day(self, item, item_index, items_id):
        '''
        Generate a time series model 
        https://www.mongodb.com/blog/post/schema-design-for-time-series-data-in-mongodb
        '''
        result = False
        try:
            ## Check if given item already exists, otherwise
            ## insert new time series model for each item ID
            keyItem                 = item[item_index]
            condition               = {item_index: keyItem}
            posts                   = self.db_handler.Find(condition)
            datetime_now            = datetime.datetime.utcnow()
            postsSize               = posts.count()
            
            ## We can receive more than one time series item
            ## to update per call in the same item
            #TODO: Do a more efficient update/insert for bulk items
            if postsSize < 1:
                ## Prepare time series model for time series
                def get_day_timeseries_model(value, datetime_now):
                    return { 
                        "timestamp_day": datetime_now.year,
                        "value": {
                            str(datetime_now.month): {
                                str(datetime_now.day) : value
                            }
                        }
                    };
                for key_item in items_id:
                    item[key_item]   = get_day_timeseries_model(item[key_item], datetime_now)
                ## Inserting time series model
                post_id             = self.db_handler.Insert(item)
                rospy.logdebug("T2:  -     Inserted time series item with hash [%s] in collection [%s]"% 
                                  (keyItem, self.db_handler.coll_name))
                result = post_id is not None
            else:
                if postsSize>1:
                    rospy.logdebug('   Warning found [%d] items for [%s]'
                                      %(postsSize, keyItem))
                for post in posts:  ## it should be only 1 post!
                    ## 1) Check if there are missing or extra keys
                    updated_missing = self.AddMissingKeys(copy.deepcopy(post), item)
                    if updated_missing:
                        rospy.logdebug('T2:    2.4.1) Added item  [%s] into DB ', keyItem)
                    else:
                        rospy.logdebug('T2:    2.4.2) DB Updated failed or no added key in item [%s]', keyItem)
                    
                    ## 2) Check if items for HASH already exists
                    ts_updated      = self.UpdateBestSeriesValue(post, 
                                                                 item, 
                                                                 item_index, 
                                                                 items_id)
                    if ts_updated:
                        rospy.logdebug('T2:    2.4.3) Time series updated for [%s]', keyItem)
                    else:
                        rospy.logdebug('T2:    2.4.4) DB Updated failed or time series not updated for [%s]', keyItem)
                    result              = updated_missing and ts_updated
                    
        except Exception as inst:
            result = False
            ros_node.ParseException(inst)
        finally:
            return result
