# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:30:22 2022

@author: lken
"""

import requests
import datetime as dt
from hashlib import blake2s

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
            ["blake2b_64bit_checksum", "INTEGER"]
        ],
        'conds': [
            "UNIQUE(mjd, blake2b_64bit_checksum)"
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
            ["blake2b_64bit_checksum", "INTEGER"]
        ],
        'conds': [
            "UNIQUE(mjd, blake2b_64bit_checksum)"
        ] # Is there a short way to include all columns as UNIQUE?
    }
    
    #%% Constructor
    def __init__(self, dbpath: str):
        super().__init__(dbpath)
        
        self.usedSrcs = None
        
    
    #%% Common use-case methods
    def update(self):
        # Download
        data, time_retrieved = self.download()
        # Loop over the sources
        for src, raw in data.items():
            # Parse it into rows with typing
            bulletins = self.parseBulletins(src, raw)
            # Create the table if necessary
            
            
            
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
        
    #%% Table handling
    def makeTable(self, src: str):
        pass
    
    def makeTable1980(self, src: str):
        pass
    
    def makeTable2000(self, src: str):
        pass
    
    def insertIntoTable(self, src: str, bulletins: list):
        pass
    
    def insertIntoTable1980(self, src: str, bulletins: list):
        pass
    
    def insertIntoTable2000(self, src: str, bulletins: list):
        pass
        
    #%% Hash functions used for comparisons
    def _hashLine(self, line: str):
        # We always strip for consistency
        line = line.strip()
        # Then hash into a short 64-bit sequence using blake2s
        hashed = blake2s(line.encode('utf-8'), digest_size=8).digest()
        
        return hashed
        
    #%% Parsing
    @staticmethod
    def parseBulletin(self, key: str, data: str):
        if '1980' in key:
            return self.parseBulletins1980(data)
        elif '2000' in key:
            return self.parseBulletins2000(data)
        else:
            raise KeyError("Key was invalid. No appropraite parse found.")
    
    
    @staticmethod
    def parseBulletins1980(self, data: str):
        bulletins = []
        # Split into lines
        data = data.split("\n")
        # Read each line according to https://maia.usno.navy.mil/ser7/readme.finals
        for line in data:
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
                
            # TODO: include hashes
                
            # We append as list of tuples, for ease of inserts later
            bulletins.append(
                (year, month, day, mjday,
                 ip_A_polar, A_pmx_arcsec, A_pmx_err_arcsec, A_pmy_arcsec, A_pmy_err_arcsec,
                 ip_A_dut1, A_dut1_sec, A_dut1_err_sec,
                 A_lod_msec, A_lod_err_msec,
                 ip_A_nutation,
                 A_dpsi_arcmsec, A_dpsi_err_arcmsec, A_deps_arcmsec, A_deps_err_arcmsec,
                 B_pmx_arcsec, B_pmy_arcsec, B_dut1_sec, B_dpsi_arcmsec, B_deps_arcmsec)    
            )
            
        return bulletins
        
                
                
    @staticmethod
    def parseBulletins2000(self, data: str):
        bulletins = []
        # Split into lines
        data = data.split("\n")
        # Read each line according to https://maia.usno.navy.mil/ser7/readme.finals2000A
        for line in data:
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
                
            # We append as list of tuples, for ease of inserts later
            bulletins.append(
                (year, month, day, mjday,
                 ip_A_polar, A_pmx_arcsec, A_pmx_err_arcsec, A_pmy_arcsec, A_pmy_err_arcsec,
                 ip_A_dut1, A_dut1_sec, A_dut1_err_sec,
                 A_lod_msec, A_lod_err_msec,
                 ip_A_nutation,
                 A_dX_arcmsec, A_dX_err_arcmsec, A_dY_arcmsec, A_dY_err_arcmsec,
                 B_pmx_arcsec, B_pmy_arcsec, B_dut1_sec, B_dX_arcmsec, B_dY_arcmsec)    
            )
            
        return bulletins
        
    
    
    
#%%
if __name__ == "__main__":
    d = BulletinDatabase("bulletins.db")
    d.setSrcs("dailyiau2000")
    data, time_retrieved = d.download()