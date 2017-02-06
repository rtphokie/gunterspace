'''
Created on Feb 4, 2017

@author: trice
'''
import requests, requests_cache
from pprint import pprint
from bs4 import BeautifulSoup
import re
import datetime
import sqlite3

requests_cache.install_cache('skyrocket_cache', backend='sqlite', expire_after=180000)

class DB():
    
    def __init__(self):
        self.conn = sqlite3.connect("gunter.sqlite")
        self.c = self.conn.cursor()
        self.create_database('launches')

    def create_database(self,table_name):
        sql = '''CREATE TABLE IF NOT EXISTS `launches` (
    `id`    TEXT,
    `date`    TEXT,
    `pad`    TEXT,
    `remark`    TEXT,
    `sitecode`    NUMERIC,
    `vehicle`    INTEGER,
    PRIMARY KEY(`id`),
    UNIQUE(id) ON CONFLICT REPLACE
);'''
        self.c.execute(sql)
        
        sql = '''CREATE TABLE IF NOT EXISTS `payloads` (
    `id`    INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
    `launchid`    TEXT,
    `name`    TEXT,
    `remark`    TEXT,
     UNIQUE(launchid, name) ON CONFLICT REPLACE
);'''
        self.c.execute(sql)
        
        sql = '''CREATE TABLE IF NOT EXISTS `sites` (
    `sitecode`    TEXT PRIMARY KEY UNIQUE,
    `name`    TEXT,
    `city`    TEXT,
    `country`    TEXT
);'''
        self.c.execute(sql)
        self.conn.commit()
      
    def add_site_row(self, ref):
        sql = '''INSERT INTO sites(sitecode, name, city, country)  VALUES("%s","%s","%s","%s") ''' % (ref['sitecode'],ref['site'], ref['city'],ref['country'])
        try:
            self.c.execute(sql)
        except sqlite3.IntegrityError as err:
            pass
        

        self.conn.commit()
        return self.c.lastrowid       


      
  
    def add_launch_row(self, ref):
        sql = '''INSERT INTO 
launches(id, date, pad, remark, sitecode, vehicle) 
VALUES('%s','%s','%s','%s','%s','%s') ''' % (ref['id'],ref['date'],ref['pad'],ref['remark'],ref['sitecode'],ref['vehicle'])
        try:
            self.c.execute(sql)
        except sqlite3.IntegrityError as err:
            pass
        for payload in ref['payload']:
            m = re.search("(.*)\s+\((.+)\)$", payload)
            if m:
                name = m.group(1)
                remark = m.group(2)
            else:
                name = payload
                remark = None
            
            sql = '''INSERT INTO payloads(launchid, name, remark) VALUES("%s","%s", "%s") ''' % (ref['id'], name, remark)
            try:
                self.c.execute(sql)
            except sqlite3.IntegrityError as err:
                pass
        self.conn.commit()
        return self.c.lastrowid       


class Year():
    def __init__(self, year):
        self.year = year
        self.countries = set()
        self.cities = set()
        self.sites = set()
        self.launchsites = dict()
        self.launches = dict()
        self.url = "http://space.skyrocket.de/doc_chr/lau%d.htm" % year
        self.r = requests.get(self.url).text
        self.soup = BeautifulSoup(self.r, "lxml")
        
        launchsites = self.soup.find_all("span",  {"class": "ls"}) #<span class="ls">
        
        for launchsite in launchsites:
            self.launchsites[launchsite.text.rstrip()] = self.parseLaunchSite(launchsite.nextSibling)
            self.launchsites[launchsite.text.rstrip()]['sitecode'] = launchsite.text.rstrip()
            
    def processListID(self, cells):
        m_id = re.search("\((.+)\)", cells[0].text)
        if m_id:
            result = "%d-%s" % (self.year, m_id.group(1))
        else:
            result = cells[0].text
        return result

    def processListDate(self, cells):
        dateatoms = cells[1].text.replace('?', '').split('.')
        date = datetime.datetime(int(dateatoms[2]), int(dateatoms[1]), int(dateatoms[0]))
        return date

    def processListVehicle(self, cells):
        try:
            vehicle = cells[3].find('a').text
        except:
            vehicle = cells[3]
        return vehicle

    def processListSite(self, cells):
        m_site1 = re.search("([A-Za-z][A-Za-z])\s([0-9A-Za-z\-]+)", cells[4].text)
        m_site2 = re.search("([A-Za-z][A-Za-z])", cells[4].text)
        m_site3 = re.search("([0-9A-Za-z\-]+)", cells[4].text)
        if m_site1:
            sitecode = m_site1.group(1)
            pad = m_site1.group(2)
        elif m_site2:
            sitecode = m_site2.group(1)
            pad = None
        elif m_site2:
            pad = m_site3.group(1)
            sitecode = None
        else:
            pad = None
            sitecode = None
        return pad, sitecode

    def processList(self):
        chronlist = self.soup.find_all("table",  {"id": "chronlist"})
        done = False
        for launch in chronlist:
            lines = launch.find_all("tr")
            for line in lines:
                if line.find("a",  {"name": "Planned"}):
                    done = True
                if not done:
                    cells = line.find_all("td")
                    if cells:
                        launchid = self.processListID(cells)
                        date = self.processListDate(cells)
                        vehicle = self.processListVehicle(cells)
                        pad, sitecode = self.processListSite(cells)
                        
                        self.launches[launchid] = {'date': date,
    #                                          'payload': cells[2].find('a').text,
                                             'payload': re.split(r'\n\s+', cells[2].find('a').text),
                                             'vehicle': vehicle,
                                             'pad': pad,
                                             'id': launchid,
                                             'sitecode': sitecode,
                                             'remark': cells[5].text,
                                             }
            
    def parseLaunchSiteFixString(self, string):
        if 'Alcantara Space Center ' in string:
            string = string.replace('Brazil', 'Maranhao, Brazil')
        if 'Plesetsk ' in string:
            string = string.replace('USSR', 'Mirny, USSR')
            string = string.replace('Russia', 'Mirny, Russia')
        return string

    def parseLaunchSite(self, string):
        string = self.parseLaunchSiteFixString(string)
        m = re.search(' = (.*)', string)
        if m:
            foo = m.group(1) 
            atoms = re.split(r',\s*(?![^()]*\))', foo) # split on commas not in parens
            country = atoms.pop()
            if len(atoms) == 3:
                state = atoms.pop()
                city = "%s, %s" % (atoms.pop(), state)
            else:
                city = atoms.pop()
            site = atoms.pop()
                
            self.countries.add(country)
            self.sites.add(site)
            self.cities.add(city)
            
            return {'country': country,
                    'city': city,
                    'site': site,
                    }


class Launches(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.launchsites = dict()
        self.launches = dict()
        self.db = DB()
        curryear = datetime.datetime.now().year
        for year in range(1957,curryear+1):
#         for year in range(2011,2012): # for testing
            print year
            if year == curryear:
                requests_cache.install_cache('skyrocket_cache', backend='sqlite', expire_after=86400) #less aggressive caching for current year

            thisyear = Year(year)
            thisyear.processList()
            self.launchsites.update(thisyear.launchsites)
            for site in thisyear.launchsites:
                self.db.add_site_row(thisyear.launchsites[site])
            for launch in thisyear.launches.keys():
                if thisyear.launches[launch]['sitecode']:
                    try:
                        thisyear.launches[launch]['site'] = thisyear.launchsites[thisyear.launches[launch]['sitecode']]
                    except:
                        pass
            self.launches.update(thisyear.launches)
        
        # update DB
        for launchid in self.launches.keys():
            self.db.add_launch_row(self.launches[launchid])
    