# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:30:22 2022

@author: lken
"""

import requests
import datetime as dt

from genericdb import Database

#%%
class BulletinDatabase(Database):
    srcs = {
        "dailyiau2000": "https://datacenter.iers.org/data/latestVersion/finals.daily.iau2000.txt",
        "dailyiau1980": "https://datacenter.iers.org/data/latestVersion/finals.daily.iau1980.txt",
        "alliau2000": "https://datacenter.iers.org/data/latestVersion/finals.all.iau2000.txt",
        "alliau1980": "https://datacenter.iers.org/data/latestVersion/finals.all.iau1980.txt",
        "dataiau2000": "https://datacenter.iers.org/data/latestVersion/finals.data.iau2000.txt",
        "dataiau1980": "https://datacenter.iers.org/data/latestVersion/finals.data.iau1980.txt"
    }
    
    #%% Table definitions
    satellite_table_fmt = {
        'cols': [
            ["time_retrieved", "INTEGER"],
            ["line1", "TEXT"],
            ["line2", "TEXT"]
        ],
        'conds': [
            "UNIQUE(line1, line2)"
        ]
    }
    
    #%% Constructor
    def __init__(self, dbpath: str):
        super().__init__(dbpath)
        
        self.usedSrcs = None
        
    
    #%% Common use-case methods
    def setSrcs(self, srckeys: list):
        if isinstance(srckeys, str):
            srckeys = [srckeys] # Make it into a list for them
        
        self.usedSrcs = {key: self.srcs[key] for key in srckeys}
        
    def download(self):
        if self.usedSrcs is None:
            raise ValueError("No sources are activated. Please call setSrcs().")
        
        data = dict()
        time_retrieved = dict()
        for key, link in self.usedSrcs.items():
            try:
                r = requests.get(link)
                data[key] = r.text
                print("Retrieved %s from %s" % (key, link))
                time_retrieved[key] = int(dt.datetime.utcnow().timestamp())
            except:
                print("Could not download from %s" % link)
        
        return data, time_retrieved
        
    #%% Parsing
    @staticmethod
    def parseBulletins1980(self, data: str):
        bulletins = dict()
        # Split into lines
        data = data.split("\n")
        # Read each line according to https://maia.usno.navy.mil/ser7/readme.finals
        for line in data:
            year = int(line[0:2])
            month = int(line[2:4])
            day = int(line[4:6])
            mjday = float(line[7:15])
            # TODO: complete
        
    @staticmethod
    def parseBulletins2000(self, data: str):
        bulletins = dict()
        # Split into lines
        data = data.split("\n")
        # Read each line according to https://maia.usno.navy.mil/ser7/readme.finals2000A
        
        
    
    
    
#%%
if __name__ == "__main__":
    d = BulletinDatabase("bulletins.db")
    d.setSrcs("dailyiau2000")
    data, time_retrieved = d.download()