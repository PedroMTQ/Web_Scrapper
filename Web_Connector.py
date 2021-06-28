

#External modules
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from os.path import isdir

import requests
from urllib.parse import quote_plus
from random import choice,randrange
import re
import sys
import os
from types import GeneratorType as generator


if sys.platform.startswith('win'):
    SPLITTER = '\\'
else:
    SPLITTER = '/'

#Module to fetch and make requests
'''
This module allows requests to be done through the standard requests package and through selenium. The latter is
aimed at websites with javascript, allowing thus the activation of scripts.
It automatically generates proxies and user agents, switching these every "allowed_number_uses".
It is also a polite fetcher as it dynamically limits the time between connections.

This method has a slower start-up while proxies and user-agents are found but allows safe and "unrestricted" database scraping

'''

def xstr(s):
    if not s:
        return ''
    if isinstance(s,generator): s=list(s)
    if isinstance(s,list):
        return ' , '.join(s)
    return str(s)

def find_path(to_search_name,to_search='file',end_dir='Web_Connector',parent_directory=None):
    past_path=str(os.getcwd())
    current_dir=os.getcwd()
    current_dir_limit=3
    c=0
    while end_dir not in str(os.getcwd()).split(SPLITTER)[-1] and c<= current_dir_limit:
        if current_dir==os.getcwd():
            c+=1
        else:
            current_dir = os.getcwd()
            c = 0
        os.chdir("..")
    for root, dirs, files in os.walk(os.getcwd()):
        parent_dir=root.split(SPLITTER)[-1]
        if to_search=='directory': target=dirs
        else: target=files
        if to_search_name in target:
            if parent_directory:
                if parent_dir==parent_directory:
                    os.chdir(past_path)
                    return os.path.join(root, to_search_name)
            else:
                os.chdir(past_path)
                return os.path.join(root, to_search_name)

class wait_for_display(object):
    def __init__(self, locator,wanted_atr,atr_value):
        self.locator = locator
        self.wanted_atr = wanted_atr
        self.atr_value = atr_value


    def __call__(self, driver):
        try:
            element = EC._find_element(driver, self.locator)
            print('waiting for display',element)
            return element.get_attribute(self.wanted_atr) == self.atr_value
        except StaleElementReferenceException:
            return False


class Web_Connector():
    def __init__(self,politeness_timer=1,
                 retry_timer=5,
                 try_limit=1,
                 try_limit_neo4j=30,
                 allowed_number_uses=200,
                 test=False,
                 omit_error_messages=False,
                 timeout=20,
                 multiplier_max_timer=3,
                 soft_js_load_delay=2,
                 hard_js_load_delay=20,
                 browser='firefox'):
        self.try_limit_neo4j = try_limit_neo4j
        self.browser=browser
        self.test=test
        self.timeout=timeout
        self.multiplier_max_timer=multiplier_max_timer
        self.politeness_timer = politeness_timer
        self.initial_politeness_timer = float(politeness_timer)
        self.retry_timer = retry_timer
        self.try_limit = try_limit
        self.request_time = time.time()
        self.allowed_number_uses=float(allowed_number_uses)
        self.proxy_uses=float(allowed_number_uses)
        self.user_agent_uses=float(allowed_number_uses)
        self.omit_error_messages=omit_error_messages
        if self.test and not self.omit_error_messages: print('THIS FETCHER IS FOR TESTING, REMEMBER TO CHANGE IT!')
        self.chromedriver_path = None
        self.geckodriver_path = None
        self.phantomjs_path=None
        self.soft_js_load_delay=soft_js_load_delay
        self.hard_js_load_delay=hard_js_load_delay
        #Reloading proxies and user agents
        if self.is_test():  self.proxies=[None]
        else:               self.proxies=self.generate_proxies()
        if self.is_test():  self.user_agents=[None]
        else:               self.user_agents=self.generate_user_agents()
        self.current_proxy=choice(self.proxies)
        self.current_user_agent=choice(self.user_agents)
        self.credentials_to_remove={'proxy':{},'user_agent':{}}
        #how many times we try the same credentials until we remove them from the database
        self.bad_credential_threshold_proxy=3
        self.bad_credential_threshold_user_agent=5

    def get_timeout(self):
        return self.timeout

    def is_test(self):
        return self.test

    def omit_errors(self):
        return self.omit_error_messages

    def get_request_time(self):
        return self.request_time


#Since not all proxies and suer-agents may be valid, it's important to allow a high number of retries in order to find a good match
###################################
############CREDENTIALS############
###################################

    def remove_credentials(self):
        if self.current_proxy not in self.credentials_to_remove['proxy']:
            self.credentials_to_remove['proxy'][self.current_proxy] = 0
        self.credentials_to_remove['proxy'][self.current_proxy] += 1
        if self.credentials_to_remove['proxy'][self.current_proxy] >= self.bad_credential_threshold_proxy:
            self.credentials_to_remove['proxy'].pop(self.current_proxy)

        if self.current_user_agent not in self.credentials_to_remove['user_agent']:
            self.credentials_to_remove['user_agent'][self.current_user_agent] = 0
        self.credentials_to_remove['user_agent'][self.current_user_agent] += 1
        if self.credentials_to_remove['user_agent'][self.current_user_agent] >= self.bad_credential_threshold_user_agent:
            self.credentials_to_remove['user_agent'].pop(self.current_user_agent)


    def get_new_proxy(self):
        #normally this shouldn't be necessary, however since multithreading is being used, proxy may be removed in a simultaneous usage of the same fetcher
        if self.current_proxy in self.proxies:
            self.proxies.remove(self.current_proxy)
        if not self.proxies: self.proxies=self.generate_proxies()
        if not self.proxies: self.current_proxy=None
        else:                self.current_proxy=choice(self.proxies)
        self.proxy_uses=self.allowed_number_uses

    def get_new_user_agent(self):
        if self.current_user_agent in self.user_agents:
            self.user_agents.remove(self.current_user_agent)
        if not self.user_agents:
            self.user_agents=self.generate_user_agents()
        self.current_user_agent=choice(self.user_agents)
        self.user_agent_uses=self.allowed_number_uses

    def renovate_permissions(self):
        self.remove_credentials()
        self.get_new_proxy()
        self.get_new_user_agent()
        print('Renovating permissions!'
                       '\nProxy: '+xstr(self.current_proxy)+''
                       '\nUser agent: '+xstr(self.current_user_agent))


    def get_proxy(self):
        if self.proxy_uses==0:
            self.get_new_proxy()
            return self.current_proxy
        else: return self.current_proxy

    def get_user_agent(self):
        if self.user_agent_uses==0:
            self.get_new_user_agent()
            return self.current_user_agent
        else: return self.current_user_agent

    def get_proxies(self):
        for i in self.proxies: yield i

    def get_user_agents(self):
        for i in self.user_agents: yield i

    # Rotation of proxy and user agents to avoid getting blocked
    def generate_proxies(self):
        #only use stored credentials once
        if not hasattr(self,'proxies'): self.proxies=[]
        if self.proxies:
            proxies=[]
            for proxy in self.get_proxies():
                proxies.append(proxy)
            self.proxies=[]
            return proxies
        proxy_urls = ['https://free-proxy-list.net/',
                      'https://www.us-proxy.org/',
                      'https://hidemy.name/en/proxy-list/',
                      'https://www.sslproxies.org',
                      'https://www.socks-proxy.net/',
                      'http://spys.one/en/']
        while proxy_urls:
            try:
                proxies=[]
                #these stopped working with requests
                url=choice(proxy_urls)
                proxy_urls.remove(url)
                response = requests.get(url,timeout=self.get_timeout())
                find_proxies= re.finditer('(\d+\.){3}\d+',response.text)
                for i in find_proxies:
                    proxies.append(i.group())
                if proxies: return proxies
                else: raise Exception
            except: pass

        return []

    def generate_user_agents(self):
        if not hasattr(self,'user_agents'): self.user_agents=[]
        #only use stored credentials once
        if self.user_agents:
            user_agents=[]
            for user_agent in self.get_user_agents():
                user_agents.append(user_agent)
            self.user_agents=[]
            return user_agents
        url = 'http://www.useragentstring.com/pages/useragentstring.php?typ=Browser'
        try:
            user_agents = []
            response = requests.get(url,timeout=self.get_timeout())
            soup = BeautifulSoup(response.text, 'lxml')
            search = soup.find_all('a')
            for i in search:
                if re.search('/index.php\?id=\d+', i['href']):
                    user_agents.append(i.text)
            return user_agents
        except: #in case website is down, we'll just use standard user-agents
            return [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.6.01001)',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.7.01001)',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.5.01003)',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0',
                'Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8',
                'Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0.1',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:11.0) Gecko/20100101 Firefox/11.0',
                'Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; .NET CLR 1.0.3705)',
                'Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0.1',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:13.0) Gecko/20100101 Firefox/13.0.1',
                'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
                'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
                'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)',
                'Opera/9.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.01',
                'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)',
                'Mozilla/5.0 (Windows NT 5.1; rv:5.0.1) Gecko/20100101 Firefox/5.0.1',
                'Mozilla/5.0 (Windows NT 6.1; rv:5.0) Gecko/20100101 Firefox/5.02',
                'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1',
                'Mozilla/4.0 (compatible; MSIE 6.0; MSIE 5.5; Windows NT 5.0) Opera 7.02 Bork-edition [en]'
                'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
                'Mozilla/5.0 (Windows NT 5.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
                'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
                'Mozilla/5.0']

###################################
###############DRIVER##############
###################################

    def get_path_driver(self,browser):
        platform= sys.platform.lower()
        if browser=='chrome':
            if not self.chromedriver_path:
                if platform.startswith('win'):
                    self.chromedriver_path= find_path('chromedriver_win64.exe')
                elif 'linux' in platform:
                    self.chromedriver_path = find_path('chromedriver_linux64')
                elif 'darwin' in platform:
                    self.chromedriver_path = find_path('chromedriver_mac64')
            return self.chromedriver_path
        elif browser=='firefox':
            if not self.geckodriver_path:
                if platform.startswith('win'):
                    self.geckodriver_path= find_path('geckodriver_win64.exe')
                elif 'linux' in platform:
                    self.geckodriver_path = find_path('geckodriver_linux64')
            return self.geckodriver_path
        elif browser=='phantomjs':
            if not self.phantomjs_path:
                if platform.startswith('win'):
                    self.phantomjs_path= find_path('phantomjs_win.exe')
                elif 'linux' in platform:
                    self.phantomjs_path = find_path('phantomjs_linux64')
            return self.phantomjs_path

    def setup_driver(self):
        if self.browser=='chrome':
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            # removing dev-tools warnings
            options.add_argument('--log-level=3')
            driver = webdriver.Chrome(executable_path=self.get_path_driver(self.browser), options=options)
            return driver,options
        elif self.browser=='firefox':
            options = webdriver.FirefoxOptions()
            #you can add your mozilla binary path here:
            #options.binary_location= '/path/to/programs/firefox/firefox'
            options.add_argument('--headless')
            # removing dev-tools warnings
            options.add_argument('--log-level=3')
            driver = webdriver.Firefox(executable_path=self.get_path_driver(self.browser), options=options)
            return driver,options
        elif self.browser=='phantomjs':
            driver = webdriver.PhantomJS(executable_path=self.get_path_driver(self.browser))
            options=None
            return driver,options

###################################
###############TIMER###############
###################################


    def get_politeness_timer(self):
        return self.politeness_timer

    def get_initial_politeness_timer(self):
        return self.initial_politeness_timer

    def get_retry_timer(self):
        return self.retry_timer

    def get_try_limit(self):
        return self.try_limit

    def set_politeness_timer(self,politeness_timer):
            self.politeness_timer=politeness_timer

    def get_randrange_retry_timer(self,denominator=4):
        allowed_range=[self.get_retry_timer()-self.get_retry_timer()/denominator,self.get_retry_timer()+self.get_retry_timer()/denominator]
        return round(randrange(int(allowed_range[0]*100),int(allowed_range[1]*100))/100,2)

    #This ensures that time between requests accompanies database latency
    def dynamic_politeness_timer(self,req_start,req_stop,c):
        """
        :param req_start: Time request was created
        :param req_stop: Time response was received
        :param c: Number of retries done so far
        :return: sets a politness timer based on the number of retries and on latency. Adding randomness for harder detection
        """
        if not req_start or not req_stop: return None
        try_limit=self.get_try_limit()
        ratio= int((c/try_limit)*100)
        multiplier=1
        if ratio in range(25,50):   multiplier=2
        elif ratio in range(50,75): multiplier=5
        if ratio > 75:              multiplier=10
        latency= req_stop-req_start
        if latency> self.get_politeness_timer():
            #pages may not give a status code caught by the request function. An unexistant page would then give a very high latency
            #we use latency<self.get_initial_politeness_timer()*3 as the threshold but it could be bigger
            if self.initial_politeness_timer < latency < self.initial_politeness_timer*self.multiplier_max_timer:
                rand_time=(randrange(0,100)/100) * latency + latency
                self.set_politeness_timer(rand_time*multiplier)
        else:
            rand_range =randrange(0,25)/100
            rand_choice=choice([rand_range,-rand_range])
            #in order to not go below the minimum time
            if latency>self.initial_politeness_timer:
                rand_time=rand_choice * latency + latency
                self.set_politeness_timer(rand_time*multiplier)
            else:
                rand_time =  rand_choice * self.initial_politeness_timer + self.initial_politeness_timer
                self.set_politeness_timer(rand_time*multiplier)
        #print('timer set for', self.get_politeness_timer())

    def set_retry_timer(self,retry_timer):
        self.retry_timer=retry_timer

    def set_try_limit(self,try_limit):
        self.try_limit=try_limit

    def update_request_time(self):
        self.request_time=time.time()

    def allow_request(self):
        #randrange for unpredictable request times
        if time.time()+self.get_politeness_timer()+self.get_politeness_timer()*(randrange(0,100)/100)>=self.get_request_time():
            self.update_request_time()
            return True
        else: return False

###################################
########EXCEPTION HANDLING#########
###################################

    #sometimes response 200 is returned , even though the page actually responds as a "bad request"
    def proper_response(self,req,url,c):
        if '400 Bad Request' in req:
            return False,url
        if 'The requested URL is malformed.' in req:
            return False,url
        return True,url

    def page_doesnt_exist(self,req):
        #drugbank response
        if 'This page doesn\'t exist. What a pain.' in req.text: return True
        else: return False

    def is_broken_link(self,page,link,current_c):
        try:
            page_source=page.text
        except:
            page_source=page.page_source
        if link and page_source:
            broken_link_pattern = re.compile('(404 - File or directory not found.)|'
                                             '(Invalid URL parameters)|'
                                             '(Failed to retrieve sequence)|'
                                             '(File not found)|'
                                             '(PUGREST\.NotFound)')
            if re.search(broken_link_pattern, page_source):
                #just to avoid executing the save command over and over
                if current_c+5>=self.try_limit:
                    if not self.omit_errors(): print('Saved as invalid URL '+link)
                return True
        return False



###################################
#############REQUESTS##############
###################################


    def generate_header(self,user_agent,referer,url):
        header = {'User-Agent': user_agent,
                      'Referer':referer,
                      }
        return header

    def print_status_code(self,status_code):
        if status_code == 400:      print('Bad request')
        elif status_code == 401:    print('Unauthorized')
        elif status_code == 403:    print('Forbidden')
        elif status_code == 404:    print('Not found')
        elif status_code == 408:    print('Request timeout')
        elif status_code == 429:    print('Too many requests')
        elif status_code == 500:    print('Internal Server Error')
        else:                       print('Client error response')


#ncbi, uniprot , chemspider, lipidmaps, pubchem, vmh, eci, rcsb, foodb,
    def try_until_catch(self,url, data=None, original_response=False,exceptional_try_limit=None):
        req_start,req_end=None,None
        while not self.allow_request():
            time.sleep(0.1)
        url = url.replace(' ', quote_plus(' '))
        error = False
        # while some ids have IDs they don't actually have a database page. This is also required to find a proxy and user agent that work
        c = 0
        referer='/'.join(url.split('/')[0:3])+'/'
        if exceptional_try_limit: try_limit=exceptional_try_limit
        else: try_limit=self.try_limit
        while c <= try_limit:
            proxy=self.get_proxy()
            if proxy: proxies={"http": proxy, "https": proxy}
            else: proxies=None
            user_agent = self.get_user_agent()
            header = self.generate_header(user_agent,referer,url)
            try:
                self.proxy_uses -= 1
                self.user_agent_uses -= 1
                req_start=time.time()
                if data:
                    #data is sent as a dictionary
                    if self.get_try_limit()-c<= 5 or self.is_test():
                        req = requests.post(url, headers={'User-Agent':'Mozilla/5.0'}, data=data,timeout=self.get_timeout())
                        #req = self.run_pycurl(url, header={'User-Agent':'Mozilla/5.0'},proxy=proxy, data=data)
                    else:
                        req = requests.post(url, headers=header, data=data, proxies=proxies,timeout=self.get_timeout())
                        #req = self.run_pycurl(url, header=header,proxy=proxy, data=data)
                else:
                    if self.get_try_limit()-c<= 5 or self.is_test():
                        req = requests.get(url, headers={'User-Agent':'Mozilla/5.0'},timeout=self.get_timeout())
                    else:
                        req = requests.get(url, headers=header, proxies=proxies,timeout=self.get_timeout())
                proper_response,url=self.proper_response(req.text, url,c)
                if not proper_response:
                    c+=1
                    raise ConnectionError
                if self.is_broken_link(req,url,c): c+=5
                if error and req.status_code == 200:
                    if not self.omit_errors(): print(url+' was successfully retrieved after '+str(c)+' retries.')
                #to set a timer which follows current website latency
                req_end=time.time()
                self.dynamic_politeness_timer(req_start,req_end,c)
                #END GOAL
                if req.status_code== 200:
                    self.current_proxy=proxy
                    self.current_user_agent=user_agent
                    if original_response: return req
                    else:                 return req.text

                #If blocked, find another proxy and user agent and try again
                else:
                    c+=1
                    self.renovate_permissions()
                    if not self.omit_errors():
                        self.print_status_code(req.status_code)
            #previous status are the most common, but there can be exceptions
            except:
                req_end=time.time()
                c+=1
                self.dynamic_politeness_timer(req_start,req_end,c)
                randrange_retry_timer=self.get_randrange_retry_timer()
                if not self.omit_errors(): print('Server error (requests-try '+str(c)+') while getting ' + url + ' , retrying again in ' + str(randrange_retry_timer) + ' seconds.')
                self.renovate_permissions()
                time.sleep(randrange_retry_timer)
                error = True

        print('Couldn\'t open link: '+url)
        return None

    def try_until_catch_selenium(self, url,exceptional_try_limit=None):
        req_start,req_end=None,None
        url = url.replace(' ', quote_plus(' '))
        while not self.allow_request():
            time.sleep(0.1)
        c = 0
        if exceptional_try_limit: try_limit=exceptional_try_limit
        else: try_limit=self.try_limit
        while c <= try_limit:
            driver, options = self.setup_driver()
            proxy = self.get_proxy()
            if proxy and options:
                options.add_argument('--proxy-server=' + proxy)
            user_agent = self.get_user_agent()
            if user_agent and options:
                options.add_argument('user-agent=' + user_agent)
            try:
                self.proxy_uses -= 1
                self.user_agent_uses -= 1
                req_start = time.time()
                driver.get(url)
                if self.is_broken_link(driver,url,c): c+=5
                proper_response,url=self.proper_response(driver.page_source, url,c)
                if not proper_response:
                    c+=1
                    driver.quit()
                    raise ConnectionError
                req_end = time.time()
                self.dynamic_politeness_timer(req_start, req_end,c)
                self.current_proxy = proxy
                self.current_user_agent = user_agent
                return driver
            except:
                c += 1
                driver.quit()
                self.dynamic_politeness_timer(req_start,req_end,c)
                randrange_retry_timer=self.get_randrange_retry_timer()
                if not self.omit_errors(): print('Server error (selenium-try  '+str(c)+') while getting ' + url + ' , retrying again in ' + str(randrange_retry_timer) + ' seconds.')
                self.renovate_permissions()
                time.sleep(randrange_retry_timer)

        print('Couldn\'t open link: '+url)
        return None

    def get_driver_selenium(self,url, script=None,xpath=None,original_response=False,timer=0,ids_to_load=[],exceptional_try_limit=None):
        driver = self.try_until_catch_selenium(url,exceptional_try_limit)
        if not driver: return None
        if script:
            if isinstance(script, str): script=[script]
            if isinstance(script, list):
                print('waiting')
                for s in script: driver.execute_script(s)
                WebDriverWait(driver,self.soft_js_load_delay)

        #to "click" on the webpage
        if xpath:
            if isinstance(xpath, str): xpath=[xpath]
            for x in range(len(xpath)):
                try:
                    xp=xpath[x]
                    id_to_load= ids_to_load[x]
                    button = driver.find_element_by_xpath(xp)
                    button.click()
                    if timer:
                        WebDriverWait(driver, timer)
                except:
                    time.sleep(self.soft_js_load_delay)
        if original_response:   return driver
        else:
            res= driver.page_source
            driver.quit()
            return res



if __name__ == '__main__':
    f=Web_Connector()
    url='https://webscraper.io/test-sites/e-commerce/allinone'

    a=f.get_driver_selenium(url)
    print('#########################################')
    print('Webpage with selenium')
    print('#########################################')
    print(a)
    print('#########################################')
    print('Webpage with requests')
    print('#########################################')
    a=f.try_until_catch(url)
    print(a)
    print('#########################################')

    #b=f.get_driver_selenium(url,original_response=True)
    #print(repr(b),repr(type(b)))
    #b.quit()
