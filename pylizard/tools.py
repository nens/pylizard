# -*- coding: utf-8 -*-
"""
Created on Fri May 21 10:50:32 2021

@author: Karl.Schutt
"""

import requests
import pandas as pd
import math
import threading
import time
from queue import Queue
import concurrent.futures
from itertools import repeat

"""
NOTE: compatible with api v3 & v4

"""


class lizard_api_collection():
    """
    Class for retrieving data from the Lizard API in general. Supported:
        - locations
        - timeseries
        - assets
    """

    def __init__(self,url,headers=None,print_log=False,page_size=100,proxies=None):
        
        self.headers = headers
        self.proxies = proxies
        self.print_log = print_log
        if url[len(url)-1]=='/':        
            self.base_url = '{}?page_size={}'.format(url, page_size)
        else:
            self.base_url = '{}&page_size={}'.format(url, page_size)
            
        if headers == None:
            if proxies==None:
                self.info = requests.get(url = self.base_url).json()
            else:
                self.info = requests.get(url = self.base_url, proxies = self.proxies).json()
        else:
            if proxies == None:
                self.info = requests.get(url = self.base_url, headers = self.headers).json()
            else:
                self.info = requests.get(url = self.base_url, headers = self.headers, proxies = self.proxies).json()
				
        self.count = self.info['count']
        self.nr_pages = math.ceil(self.count/page_size) 
        if self.print_log!=False:
            print("Aantal assets: {}".format(self.count))
            print("Aantal pagina's: {}".format(self.nr_pages))
        self.results = []
        self.end = False
        self.queue = Queue()
        self.threads = []
        self.lock = threading.Lock()
        self.page = 0
        self.prepare()
        self.succes = 0 # Aantal pagina's met geslaagde download
        self.fail = 0 # Aantal pagina's met gefaalde download

    def download(self,data,dummy='dummy'):

        page, url = data

        try:
            if self.proxies != None:
                data = requests.get(url = url, headers = self.headers)
				
            else:
                data = requests.get(url = url, headers = self.headers, proxies = self.proxies)

            self.succes += 1
            data = data.json()['results']
            message = 'download page {} succeeded'.format(page)
        except:
            data = []
            message = 'download page {} failed'.format(page)
            self.fail += 1
            
        
        self.lock.acquire()
        self.results+=data
        self.page+=1
        self.lock.release()

        return(message)
    
    def prepare(self):
        self.proces_input = []
        for page in range(self.nr_pages):
            true_page = page+1 # Het echte paginanummer wordt aan de import thread gekoppeld
            url = self.base_url+'&page={}'.format(true_page)
            item = [true_page,url]
            self.proces_input = self.proces_input +[item]
            #self.queue.put(item) # Het echtepaginanummer en url in de queue toevoegen. 

    def get(self,nr_threads=10):
        self.nr_threads=nr_threads
        if self.print_log!=False:
            print("Download started")  
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        if self.nr_threads >= self.nr_pages:
           self.nr_threads = self.nr_pages 
        if len(self.proces_input) == 0:
            self.results = pd.DataFrame()
            return('No data to download')
        with concurrent.futures.ThreadPoolExecutor(max_workers = self.nr_threads) as executor:
            for result in executor.map(self.download, self.proces_input, repeat(self)):
                if self.print_log!=False:
                    print(result)
        
        if self.print_log!=False:
            print('Download finished')
        

        self.results = pd.DataFrame(self.results)
        self.clear() 
        return('Download succeeded')
 
    def clear(self):
        self.page = 1
        self.end = False
        self.queue = Queue()
        self.threads = []


"""
NOTE: compatible with api v4

"""

class lizard_timeseries():
    """
    Class for retrieving timeseries events data from the Lizard API 
    """

    def __init__(self,uuid,headers,page_size=10000,print_log=False, startdate=None,enddate=None,startdate_modified=None, enddate_modified=None, base_url=None):
        
        self.headers = headers
        self.print_log=print_log
        
        if base_url == None:
            base_url = 'https://demo.lizard.net'

        if startdate == None and enddate == None:
            self.base_url = base_url+'/api/v4/timeseries/{}/events/?page_size={}'.format(uuid, page_size)
        elif startdate == None:
            self.base_url = base_url+'/api/v4/timeseries/{}/events/?time__lte={}&page_size={}'.format(uuid,enddate, page_size)
        elif enddate == None:
            self.base_url = base_url+'/api/v4/timeseries/{}/events/?time__gte={}&page_size={}'.format(uuid,startdate, page_size)
        else:
            self.base_url = base_url+'/api/v4/timeseries/{}/events/?time__gte={}&time__lte={}&page_size={}'.format(uuid,startdate,enddate, page_size)
        if self.print_log!=False:        
            print(self.base_url)
			
        if startdate_modified != None:
            self.base_url = self.base_url + '&last_modified__gte={}'.format(startdate_modified)	
        if enddate_modified != None:
            self.base_url = self.base_url + '&last_modified__lte={}'.format(enddate_modified)				
			
        self.info = requests.get(url = self.base_url, headers = self.headers).json()
        self.count = self.info['count']
        self.nr_pages = math.ceil(self.count/page_size) 
        if self.print_log!=False:
            print(self.nr_pages)
        self.results = []
        self.end = False
        self.N2 = 50
        self.queue = Queue()
        self.queue2 = Queue(self.N2)
        self.threads = []
        self.lock = threading.Lock()
        self.page = 0
        self.prepare()
        self.succes = 0 # Aantal pagina's met geslaagde download
        self.fail = 0 # Aantal pagina's met gefaalde download


    def download(self, data, dummy = 'dummy'):
        page, url = data
        try:
            data = requests.get(url = url, headers = self.headers)
            if self.print_log!=False:
                print(data)
            self.succes += 1
            data = data.json()['results']
            message = 'stored data from page {}'.format(page)
        except:
            data = []
            self.fail +=1
            message = 'data from page {} not stored'.format(page)

        
        self.lock.acquire()
        self.results+=data
        self.page+=1
        self.lock.release()
			
        return(message)
		    
    def prepare(self):
        self.proces_input = []
        for page in range(self.nr_pages):
            true_page = page+1 # Het echte paginanummer wordt aan de import thread gekoppeld
            url = self.base_url+'&page={}'.format(true_page)
            item = [true_page,url]
            if self.print_log!=False:
                print(item)
            self.proces_input = self.proces_input+[item]

    def get(self,nr_threads=10):
        self.nr_threads = nr_threads
        if self.print_log!=False:
            print("Download started")  
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        if self.nr_threads >= self.nr_pages:
           self.nr_threads = self.nr_pages
           
        if len(self.proces_input) == 0:
            self.results = pd.DataFrame()
            return('No data to download')
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = self.nr_threads) as executor:
            for result in executor.map(self.download, self.proces_input, repeat(self)):
                if self.print_log!=False:
                    print(result)
        
        if self.print_log!=False:
            print('Download finished')

        self.results = pd.DataFrame(self.results)

        if len(self.results)==0:
            self.results = pd.DataFrame(columns = ['time','value','flag','validation_code','comment','detection_limit'])
        else:
            self.results = pd.DataFrame(self.results)
        # Maak na afloop alles netjes schoon:
        self.clear()   
        return('Download succeeded')


    def clear(self):
        self.page = 1
        self.end = False
        self.queue = Queue()
        self.threads = []





