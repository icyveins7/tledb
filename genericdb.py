# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:23:06 2022

@author: lken
"""

import sqlite3 as sq

#%% 
# This is meant to be an interface to contain common helper functions, to be inherited by other classes.
# Do not initialise this directly.
class Database:
    def __init__(self, dbpath: str):
        self.dbpath = dbpath
        self.con = sq.connect(self.dbpath)
        self.cur = self.con.cursor()
        
    def commit(self):
        # Simple redirect for brevity
        self.con.commit()
        
    #%% Helper functions (generally don't need to call these externally)
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