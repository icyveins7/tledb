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
import numpy as np

from genericdb import Database

#%%
class TleDatabase(Database):
    '''
    Represents a database of TLEs, ordered by sources (which is a key-value dictionary) and satellite names.
    '''
    
    srcs = {
        'active': "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle",
        'stations': "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=tle",
        'geo': "https://celestrak.org/NORAD/elements/gp.php?GROUP=geo&FORMAT=tle",
        'starlink': "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle",
        'gnss': "https://celestrak.org/NORAD/elements/gp.php?GROUP=gnss&FORMAT=tle",
        'cubesat': "https://celestrak.org/NORAD/elements/gp.php?GROUP=cubesat&FORMAT=tle"
    } # Maybe can generate the link, if the celestrak website continues this format
    
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
    
    #%% Constructor and other miscellaneous methods
    def __init__(self, dbpath: str):
        '''
        Instantiates a database on the file system.

        Parameters
        ----------
        dbpath : str
            File path of the database.
        '''
        super().__init__(dbpath)
        self.usedSrcs = None
        
    #%% Discovery methods
    def getAvailableSrcs(self):
        '''
        Returns the dictionary of keys and links used for downloading the TLE source data.
        '''
        return self.srcs
    
    def setAvailableSrcs(self, newsrcs: dict):
        '''
        Sets the dictionary of keys and links used for downloading the TLE source data.
        Generally not needed unless there are sudden changes to the hyperlinks, or using custom sources.
        '''
        self.srcs = newsrcs
        
    def getSatellites(self, remove_src: bool=True):
        '''
        Get a list of satellites that the database currently contains.

        Parameters
        ----------
        remove_src : bool, optional
            There may be repeats of the same satellite from different TLE sources.
            Set this to True if the source is unimportant.
            Set this to False and the source will be kept.
            The default is True.

        Returns
        -------
        set or dict
            Returns a set if remove_src is True.
            Returns a dict if remove_src is False, with keys specified by the source names.
        '''
        
        stmt = 'select name from sqlite_master where type="table"'
        self.cur.execute(stmt)
        results = [i[0] for i in self.cur.fetchall()]
        
        # Return a set of strings (may have had repeated satellites in different sources)
        if remove_src:
            # First cleave off the source name
            results = [i.split("_", 1)[1] for i in results]
            # Then take the set
            return set(results)
        
        else:
            # Convert into a dictionary
            resultsdict = dict()
            for result in results:
                src, name = result.split("_", 1)
                if src not in resultsdict:
                    resultsdict[src] = [name]
                else:
                    resultsdict[src].append(name)
                
            return resultsdict
        
    #%% Common use-case methods
    def update(self, verbose: bool=True):
        '''
        Downloads, parses, and then inserts the TLE data that was configured with setSrcs().
        '''
        # Download
        data, time_retrieved = self.download()
        # Parse the data
        alltles = self.parseTleDataSrcs(data)
        # Insert rows
        for src, tles in alltles.items():
            # Get the time for this source
            src_tr = time_retrieved[src]
            # Iterate over individual satellites
            for name, tlelines in tles.items():
                if verbose:
                    print("Updating %s" % (name))
                # Create table if necessary
                self.makeSatelliteTable(src, name)
                # Insert into it
                self.insertSatelliteTle(src, name, src_tr, tlelines[0], tlelines[1])
                
        # Commit changes
        self.commit()
        
    def loadTleFile(self, filepath: str, src: str, time_retrieved: int=None):
        data = dict()
        with open(filepath, "r") as fid:
            data[src] = fid.read()
        if time_retrieved is None:
            time_retrieved = int(dt.datetime.utcnow().timestamp())
            
        # Parse the data
        alltles = self.parseTleDataSrcs(data)
        
        # Insert rows
        for src, tles in alltles.items():
            # Get the time for this source
            src_tr = time_retrieved[src]
            # Iterate over individual satellites
            for name, tlelines in tles.items():
                print("Updating %s" % (name))
                # Create table if necessary
                self.makeSatelliteTable(src, name)
                # Insert into it
                self.insertSatelliteTle(src, name, src_tr, tlelines[0], tlelines[1])
                
        # Commit changes
        self.commit()
        
    
    def setSrcs(self, srckeys: list):
        '''
        Selects the source hyperlinks that will be used when downloading new TLE data.

        Parameters
        ----------
        srckeys : list or str
            A list of strings or a single string, which must match the keys provided in srcs.
            See getAvailableSrcs/setAvailableSrcs() for more info.
        '''
        if isinstance(srckeys, str):
            srckeys = [srckeys] # Make it into a list for them
        
        self.usedSrcs = {key: self.srcs[key] for key in srckeys}
        
    def download(self):
        '''
        Downloads the raw TLE text data from the activated sources from setSrcs().

        Raises
        ------
        ValueError
            If setSrcs() has not been called i.e. no sources activated yet.

        Returns
        -------
        data : dict
            This dictionary contains the raw text data with keys matching the activated sources.
        time_retrieved : dict
            This dictionary contains the time of download with keys matching the activated sources.
        '''
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
            
    #%% Helper methods
    def _makeTableName(self, src: str, name: str):
        return "%s_%s" % (src, name)
    
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
            alltles[srckey] = tles
            
        return alltles
        
        
    #%% Individual satellite tables
    def makeSatelliteTable(self, src: str, name: str):
        stmt = 'create table if not exists "%s"(%s)' % (
            self._makeTableName(src, name), self._makeTableStatement(self.satellite_table_fmt))
        # print(stmt)
        self.cur.execute(stmt)
        self.con.commit()
        
    def insertSatelliteTle(self, src: str, name: str, time_retrieved: int, line1: str, line2: str):
        stmt = 'insert or replace into "%s" values(%s)' % (
            self._makeTableName(src, name), self._makeQuestionMarks(self.satellite_table_fmt))
        # print(stmt)
        self.cur.execute(stmt, (time_retrieved, line1, line2))
        
    def getSatelliteTle(self, name: str, nearest_time_retrieved: int=None, src: str=None):
        # Get at the current time if unspecified
        nearest_time_retrieved = int(dt.datetime.utcnow().timestamp()) if nearest_time_retrieved is None else nearest_time_retrieved
        
        # Satellites can be repeated in different sources, so extract from given source if specified
        if src is not None:
            table = self._makeTableName(src, name)
            stmt = 'select * from "%s" where time_retrieved order by ABS(? - time_retrieved) limit 1' % (self._makeTableName(src, name))
            self.cur.execute(stmt, (nearest_time_retrieved, ))
            results = self.cur.fetchone()
            
        else:
            # Search all tables that contain the name
            stmt = "select name from sqlite_master where name LIKE '%%%s%%' and type='table'" % (name) # Remember to escape the % signs
            self.cur.execute(stmt)
            tables = [i[0] for i in self.cur.fetchall()] # Unpack the tuples
            
            # Pick the one that is closest
            results = []
            for table in tables:
                stmt = 'select * from "%s" where time_retrieved order by ABS(? - time_retrieved) limit 1' % (table)
                self.cur.execute(stmt, (nearest_time_retrieved, ))
                results.append(self.cur.fetchone())
                
            # Compute the ordering
            ordering = np.abs([i[0]-nearest_time_retrieved for i in results])
            idx = np.argmin(ordering)
            
            # Extract the result
            results = results[idx]
   
        return results, table
        
    
        
#%%
if __name__ == "__main__":
    d = TleDatabase("tles.db")
    
    #%% Typical use-case
    d.setSrcs(['geo', 'stations'])
    d.update()
    
    results, table = d.getSatelliteTle('MUOS-3')
    
    #%% Debugging
    # d.setSrcs('geo')
    # data, time_retrieved = d.download()
    # alltles = d.parseTleDataSrcs(data)
