# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:30:22 2022

@author: lken
"""

import requests
import datetime as dt
from hashlib import blake2s
import sqlite3 as sq

from sew import Database
# from genericdb import Database

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
    bulletins1980_table_fmt = {
        'cols': [
            ["time_retrieved", "INTEGER"],
            ["year", "INTEGER"],
            ["month", "INTEGER"],
            ["day", "INTEGER"],
            ["mjd", "REAL"],
            ["ip_A_polar", "TEXT"],
            ["A_pmx_arcsec", "REAL"],
            ["A_pmx_err_arcsec", "REAL"],
            ["A_pmy_arcsec", "REAL"],
            ["A_pmy_err_arcsec", "REAL"],
            ["ip_A_dut1", "TEXT"],
            ["A_dut1_sec", "REAL"],
            ["A_dut1_err_sec", "REAL"],
            ["A_lod_msec", "REAL"],
            ["A_lod_err_msec", "REAL"],
            ["ip_A_nutation", "TEXT"],
            ["A_dpsi_arcmsec", "REAL"],
            ["A_dpsi_err_arcmsec", "REAL"],
            ["A_deps_arcmsec", "REAL"],
            ["A_deps_err_arcmsec", "REAL"],
            ["B_pmx_arcsec", "REAL"],
            ["B_pmy_arcsec", "REAL"],
            ["B_dut1_sec", "REAL"],
            ["B_dpsi_arcmsec", "REAL"],
            ["B_deps_arcmsec", "REAL"],
            ["blake2b_32bit_checksum", "INTEGER"]
        ],
        'conds': [
            "UNIQUE(mjd, blake2b_32bit_checksum)"
        ] # Is there a short way to include all columns as UNIQUE?
    }
    
    bulletins2000_table_fmt = {
        'cols': [
            ["time_retrieved", "INTEGER"],
            ["year", "INTEGER"],
            ["month", "INTEGER"],
            ["day", "INTEGER"],
            ["mjd", "REAL"],
            ["ip_A_polar", "TEXT"],
            ["A_pmx_arcsec", "REAL"],
            ["A_pmx_err_arcsec", "REAL"],
            ["A_pmy_arcsec", "REAL"],
            ["A_pmy_err_arcsec", "REAL"],
            ["ip_A_dut1", "TEXT"],
            ["A_dut1_sec", "REAL"],
            ["A_dut1_err_sec", "REAL"],
            ["A_lod_msec", "REAL"],
            ["A_lod_err_msec", "REAL"],
            ["ip_A_nutation", "TEXT"],
            ["A_dX_arcmsec", "REAL"],
            ["A_dX_err_arcmsec", "REAL"],
            ["A_dY_arcmsec", "REAL"],
            ["A_dY_err_arcmsec", "REAL"],
            ["B_pmx_arcsec", "REAL"],
            ["B_pmy_arcsec", "REAL"],
            ["B_dut1_sec", "REAL"],
            ["B_dX_arcmsec", "REAL"],
            ["B_dY_arcmsec", "REAL"],
            ["blake2b_32bit_checksum", "INTEGER"]
        ],
        'conds': [
            "UNIQUE(mjd, blake2b_32bit_checksum)"
        ] # Is there a short way to include all columns as UNIQUE?
    }
    
    #%% Constructor
    def __init__(self, dbpath: str):
        super().__init__(dbpath)
        
        self.usedSrcs = None
        
        # We enable Rows for this
        self.con.row_factory = sq.Row
        
    
    #%% Common use-case methods
    def update(self):
        # Download
        data, time_retrieved = self.download()
        # Loop over the sources
        for src, raw in data.items():
            # Parse it into rows with typing
            bulletins = self.parseBulletins(src, raw)
            # Create the table if necessary
            self.makeTable(src)
            # Insert the bulletins
            self.insertIntoTable(src, bulletins, time_retrieved[src])
            
        # Commit changes
        self.commit()
        
        # Return for debugging purposes
        return data, time_retrieved 
    
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
        
    #%% Table handling
    def makeTable(self, src: str):
        if '1980' in src:
            self.makeTable1980(src)
        elif '2000' in src:
            self.makeTable2000(src)
        else:
            raise ValueError("Key was invalid. No appropriate table found.")
            
    
    def makeTable1980(self, src: str):
        stmt = "create table if not exists %s(%s)" % (
            src,
            self._makeTableStatement(self.bulletins1980_table_fmt))
        # print(stmt)
        
        self.execute(stmt)
        self.commit()
        
    
    def makeTable2000(self, src: str):
        stmt = "create table if not exists %s(%s)" % (
            src,
            self._makeTableStatement(self.bulletins2000_table_fmt))
        # print(stmt)
        
        self.execute(stmt)
        self.commit()
    
    def insertIntoTable(self, src: str, bulletins: list, time_retrieved: int, replace: bool=False):
        if '1980' in src:
            self.insertIntoTable1980(src, bulletins, time_retrieved, replace)
        elif '2000' in src:
            self.insertIntoTable2000(src, bulletins, time_retrieved, replace)
        else:
            raise ValueError("Key was invalid. No appropriate table found.")
    
    def insertIntoTable1980(self, src: str, bulletins: list, time_retrieved: int, replace: bool=False):
        stmt = "insert%s into %s values(%s)" % (
            " or replace" if replace else "",
            src,
            self._makeQuestionMarks(self.bulletins1980_table_fmt))
        # print(stmt)
        # We use generator expression to stitch the time retrieved
        try:
            self.executemany(stmt, ((time_retrieved, *bulletin) for bulletin in bulletins))
            self.commit()
        except sq.IntegrityError as e:
            print("Skipping due to unique constraint failure.")
    
    def insertIntoTable2000(self, src: str, bulletins: list, time_retrieved: int, replace: bool=False):
        stmt = "insert%s into %s values(%s)" % (
            " or replace" if replace else "",
            src,
            self._makeQuestionMarks(self.bulletins2000_table_fmt))
        # print(stmt)
        # We use generator expression to stitch the time retrieved
        try:
            self.executemany(stmt, ((time_retrieved, *bulletin) for bulletin in bulletins))
            self.commit()
        except sq.IntegrityError as e:
            print("Skipping due to unique constraint failure.")
            
    ######### These getters are a bit useless by themselves, usually you would want to extract the latest values for each individual variable
    def getBulletin1980(self, src: str, nearest_time_retrieved: int=None):
        # Get at the current time if unspecified
        nearest_time_retrieved = int(dt.datetime.utcnow().timestamp()) if nearest_time_retrieved is None else nearest_time_retrieved
        
        # Extract the entire row
        stmt = "select * from %s where %s order by ABS(? - time_retrieved) limit 1" % (
            src,
            self._makeNotNullConditionals(self.bulletins1980_table_fmt['cols'][13:])) 
        # This is an easy way to just select those that are not null, but depends on the definition and order of the format dictionary
        # print(stmt)
        self.execute(stmt, (nearest_time_retrieved,))
        results = self.cur.fetchall()
        return results
    
    def getBulletin2000(self, src: str, nearest_time_retrieved: int=None):
        # Get at the current time if unspecified
        nearest_time_retrieved = int(dt.datetime.utcnow().timestamp()) if nearest_time_retrieved is None else nearest_time_retrieved
        
        # Extract the entire row
        stmt = "select * from %s where %s order by ABS(? - time_retrieved) limit 1" % (
            src,
            self._makeNotNullConditionals(self.bulletins2000_table_fmt['cols'][13:])) 
        # This is an easy way to just select those that are not null, but depends on the definition and order of the format dictionary
        # print(stmt)
        self.execute(stmt, (nearest_time_retrieved,))
        results = self.fetchall()
        return results
    
    def getPolMotionDut1(self, src: str, year: int, month: int, day: int):
        # Extract each variable individually, and always pick the latest time_retrieved where it's not null
        # These 3 come together, and are always present
        stmt = "select time_retrieved, A_pmx_arcsec, A_pmy_arcsec, A_dut1_sec from %s where year=? and month=? and day=? ORDER BY time_retrieved DESC limit 1" % src
        self.execute(stmt, (year, month, day))
        try:
            tr0, pmx_arcsec, pmy_arcsec, dut1_sec = self.fetchone()
            return tr0, pmx_arcsec, pmy_arcsec, dut1_sec
        except TypeError as e:
            raise TypeError("No results; make sure year/month/day exists. %s" % str(e))
            
    def getLod(self, src: str, mjday: float):
        # LOD may not be present, so the best case is to list in order of mjday (which is generally monotonically increasing)
        # and then find the nearest date which is not empty
        stmt = "select time_retrieved, mjd, A_lod_msec from %s where A_lod_msec is not null ORDER BY ABS(mjd-?) ASC, time_retrieved DESC limit 1" % src
        self.execute(stmt, (mjday,))
        try:
            tr0, mjd, lod_msec = self.fetchone()
            return tr0, mjd, lod_msec
        except TypeError as e:
            raise TypeError("No results; maybe check mjday value? %s" % str(e))
            
    def getMjday(self, src: str, year: int, month: int, day: int):
        # Use this if you wish to get the mjday for a calendar date
        stmt = "select mjd from %s where year=? and month=? and day=? limit 1" % (src)
        self.execute(stmt, (year, month, day))
        try:
            mjday, = self.fetchone()
            return mjday
        except TypeError as e:
            raise TypeError("Could not find corresponding mjday. %s" % str(e))
        
    def getTeme2EcefParams(self, src: str, year: int, month: int, day: int):
        # Combine all the above extractors in one convenient method
        mjday = self.getMjday(src, year, month, day)
        tr_pol, pmx_arcsec, pmy_arcsec, dut1_sec = self.getPolMotionDut1(src, year, month, day)
        tr_lod, mjd_actual, lod_msec = self.getLod(src, mjday)
        return tr_pol, pmx_arcsec, pmy_arcsec, dut1_sec, tr_lod, mjd_actual, lod_msec
        
    #%% Hash functions used for comparisons
    @staticmethod
    def _hashLine(line: str):
        # We always strip for consistency
        line = line.strip()
        # Then hash into a short 32-bit sequence using blake2s
        # Note that we use a short bit sequence so that sqlite can convert it from python integers
        hashed = blake2s(line.encode('utf-8'), digest_size=7).digest()
        # Save as integer
        hashed = int.from_bytes(hashed, 'big')
        
        return hashed
        
    #%% Parsing
    @staticmethod
    def parseBulletins(key: str, data: str):
        if '1980' in key:
            return BulletinDatabase.parseBulletins1980(data)
        elif '2000' in key:
            return BulletinDatabase.parseBulletins2000(data)
        else:
            raise KeyError("Key was invalid. No appropraite parse found.")
    
    
    @staticmethod
    def parseBulletins1980(data: str):
        bulletins = []
        # Split into lines
        data = data.split("\n")
        # Read each line according to https://maia.usno.navy.mil/ser7/readme.finals
        for line in data:
            if len(line) < 79:
                continue
            
            year = int(line[0:2])
            month = int(line[2:4])
            day = int(line[4:6])
            mjday = float(line[7:15])
            
            ip_A_polar = str(line[16])
            A_pmx_arcsec = float(line[18:27])
            A_pmx_err_arcsec = float(line[27:36])
            A_pmy_arcsec = float(line[37:46])
            A_pmy_err_arcsec = float(line[46:55])
            
            ip_A_dut1 = str(line[57])
            A_dut1_sec = float(line[58:68])
            A_dut1_err_sec = float(line[68:78])
            
            # From here onwards, the fields may be blank, in which case we save Nones
            try:
                A_lod_msec = float(line[79:86])
                A_lod_err_msec = float(line[86:93])
            except:
                A_lod_msec = None
                A_lod_err_msec = None
            
            # This is another group that appears together
            try:
                ip_A_nutation = str(line[95])
                A_dpsi_arcmsec = float(line[97:106])
                A_dpsi_err_arcmsec = float(line[106:115])
                A_deps_arcmsec = float(line[116:125])
                A_deps_err_arcmsec = float(line[125:134])
            except:
                ip_A_nutation = None
                A_dpsi_arcmsec = None
                A_dpsi_err_arcmsec = None
                A_deps_arcmsec = None
                A_deps_err_arcmsec = None
                
            # And the final group from bulletin B
            try:
                B_pmx_arcsec = float(line[134:144])
                B_pmy_arcsec = float(line[144:154])
                B_dut1_sec = float(line[154:165])
                B_dpsi_arcmsec = float(line[165:175])
                B_deps_arcmsec = float(line[175:185])
            except:
                B_pmx_arcsec = None
                B_pmy_arcsec = None
                B_dut1_sec = None
                B_dpsi_arcmsec = None
                B_deps_arcmsec = None
                
            # Hash it to easily test uniqueness
            hashed = BulletinDatabase._hashLine(line)
                
            # We append as list of tuples, for ease of inserts later
            bulletins.append(
                (year, month, day, mjday,
                 ip_A_polar, A_pmx_arcsec, A_pmx_err_arcsec, A_pmy_arcsec, A_pmy_err_arcsec,
                 ip_A_dut1, A_dut1_sec, A_dut1_err_sec,
                 A_lod_msec, A_lod_err_msec,
                 ip_A_nutation,
                 A_dpsi_arcmsec, A_dpsi_err_arcmsec, A_deps_arcmsec, A_deps_err_arcmsec,
                 B_pmx_arcsec, B_pmy_arcsec, B_dut1_sec, B_dpsi_arcmsec, B_deps_arcmsec,
                 hashed)    
            )
            
        return bulletins
        
                
                
    @staticmethod
    def parseBulletins2000(data: str):
        bulletins = []
        # Split into lines
        data = data.split("\n")
        # Read each line according to https://maia.usno.navy.mil/ser7/readme.finals2000A
        for line in data:
            if len(line) < 79:
                continue
            
            year = int(line[0:2])
            month = int(line[2:4])
            day = int(line[4:6])
            mjday = float(line[7:15])
            
            ip_A_polar = str(line[16])
            A_pmx_arcsec = float(line[18:27])
            A_pmx_err_arcsec = float(line[27:36])
            A_pmy_arcsec = float(line[37:46])
            A_pmy_err_arcsec = float(line[46:55])
            
            ip_A_dut1 = str(line[57])
            A_dut1_sec = float(line[58:68])
            A_dut1_err_sec = float(line[68:78])
            
            # From here onwards, the fields may be blank, in which case we save Nones
            try:
                A_lod_msec = float(line[79:86])
                A_lod_err_msec = float(line[86:93])
            except:
                A_lod_msec = None
                A_lod_err_msec = None
            
            # This is another group that appears together
            try:
                ip_A_nutation = str(line[95])
                A_dX_arcmsec = float(line[97:106]) # Here is where it differs from 1980
                A_dX_err_arcmsec = float(line[106:115])
                A_dY_arcmsec = float(line[116:125])
                A_dY_err_arcmsec = float(line[125:134])
            except:
                ip_A_nutation = None
                A_dX_arcmsec = None
                A_dX_err_arcmsec = None
                A_dY_arcmsec = None
                A_dY_err_arcmsec = None
                
            # And the final group from bulletin B
            try:
                B_pmx_arcsec = float(line[134:144])
                B_pmy_arcsec = float(line[144:154])
                B_dut1_sec = float(line[154:165])
                B_dX_arcmsec = float(line[165:175]) # This also differs from 1980
                B_dY_arcmsec = float(line[175:185])
            except:
                B_pmx_arcsec = None
                B_pmy_arcsec = None
                B_dut1_sec = None
                B_dX_arcmsec = None
                B_dY_arcmsec = None
                
            # Hash it to easily test uniqueness
            hashed = BulletinDatabase._hashLine(line)
                
            # We append as list of tuples, for ease of inserts later
            bulletins.append(
                (year, month, day, mjday,
                 ip_A_polar, A_pmx_arcsec, A_pmx_err_arcsec, A_pmy_arcsec, A_pmy_err_arcsec,
                 ip_A_dut1, A_dut1_sec, A_dut1_err_sec,
                 A_lod_msec, A_lod_err_msec,
                 ip_A_nutation,
                 A_dX_arcmsec, A_dX_err_arcmsec, A_dY_arcmsec, A_dY_err_arcmsec,
                 B_pmx_arcsec, B_pmy_arcsec, B_dut1_sec, B_dX_arcmsec, B_dY_arcmsec,
                 hashed)    
            )
            
        return bulletins
        
    
    
    
#%%
if __name__ == "__main__":
    d = BulletinDatabase("bulletins.db")
    d.setSrcs(["dailyiau2000", "dailyiau1980"])
    # data, time_retrieved = d.download()
    # data = data['dailyiau2000']
    # bulletins = d.parseBulletins('dailyiau2000', data)
    
    # data, time_retrieved = d.update()
    
    # bulletin = d.getBulletin1980('dailyiau1980')
    
    #%% Test the extraction
    time_retrieved_pmdut, pmx_arcsec, pmy_arcsec, dut1_sec = d.getPolMotionDut1('dailyiau1980', 22, 12, 7)
    mjday_target = d.getMjday('dailyiau1980', 22, 12, 7)
    time_retrieved_lod, mjday_actual, lod_msec = d.getLod('dailyiau1980',59920.0)
    
    #%% Test convenient extraction
    tr_pol, pmx_arcsec1, pmy_arcsec1, dut1_sec1, tr_lod, mjd_actual, lod_msec1 = d.getTeme2EcefParams('dailyiau1980', 22, 12, 7)