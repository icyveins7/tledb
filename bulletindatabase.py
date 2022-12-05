# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:30:22 2022

@author: lken
"""

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
    
    #%%
    def __init__(self, dbpath: str):
        super().__init__(dbpath)
        
    
    