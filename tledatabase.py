#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 19:12:45 2022

@author: seoxubuntu
"""

import requests
import sqlite3 as sq
import re
import datetime as dt

#%%
class TleDatabase:
    srcs = {
        'geo': "https://celestrak.org/NORAD/elements/gp.php?GROUP=geo&FORMAT=tle"
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
        self.usedSrcs = None
        
        self.dbpath = dbpath
        self.con = sq.connect(self.dbpath)
        self.cur = self.con.cursor()
        
    def commit(self):
        # Simple redirect for brevity
        self.con.commit()
        
    #%% Common use-case methods
    def update(self, verbose: bool=True):
        # Download
        data, time_retrieved = self.download()
        # Parse the data
        alltles = self.parseTleDataSrcs(data)
        # Insert rows
        for name, tle in alltles.items():
            print("Updating %s" % (name))
            # Create table if necessary
            self.makeSatelliteTable(name)
            # Insert into it
            self.insertSatelliteTle(name, time_retrieved[name], tle[0], tle[1])
            
        # Commit changes
        self.commit()
        
    
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
            
        
    
    #%% Helper functions
    def _makeTableColumns(self, fmt: dict):
        return ', '.join([' '.join(i) for i in fmt['cols']])
    
    def _makeTableConditions(self, fmt: dict):
        return ', '.join(fmt['conds'])
    
    def _makeTableStatement(self, fmt: dict):
        return "%s, %s" % (
                self._makeTableColumns(fmt),
                self._makeTableConditions(fmt)
            )
    
    def _makeQuestionMarks(self, fmt: dict):
        return ','.join(["?"] * len(fmt['cols']))
    
    #%% TLE parsing
    @staticmethod # allow calls from outside a class object
    def parseTleData(datasrc: str):
        tles = dict()
        currentSat = None
        for line in datasrc.strip().split("\n"): # Split into a list of lines
            if not re.match("\\d \\d+", line): # Matches for the two lines
                currentSat = line.strip()
                tles[currentSat] = []
            else: # Otherwise it's a name (hopefully)
                tles[currentSat].append(line.strip())
                assert(len(tles[currentSat]) <= 2)
                
        return tles
    
    @staticmethod
    def parseTleDataSrcs(data: dict):
        alltles = dict()
        # Iterate over all sources
        for srckey in data:
            tles = TleDatabase.parseTleData(data[srckey])
            # Merge into the collector
            alltles = {**alltles, **tles}
            
        return alltles
        
        
    #%% Individual satellite tables
    def makeSatelliteTable(self, name: str):
        stmt = 'create table if not exists "%s"(%s)' % (name, self._makeTableColumns(self.satellite_table_fmt))
        print(stmt)
        self.cur.execute(stmt)
        self.con.commit()
        
    def insertSatelliteTle(self, name: str, time_retrieved: int, line1: str, line2: str):
        stmt = 'insert or replace into "%s" values(%s)' % (name, self._makeQuestionMarks(self.satellite_table_fmt))
        print(stmt)
        self.cur.execute(stmt, (time_retrieved, line1, line2))
    
        
    
if __name__ == "__main__":
    d = TleDatabase("tles.db")
    d.setSrcs('geo')
    d.update()
