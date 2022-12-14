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
        
        # We redirect a few calls for brevity
        self.execute = self.cur.execute
        self.executemany = self.cur.executemany
        self.commit = self.con.commit
        self.fetchone = self.cur.fetchone
        self.fetchall = self.cur.fetchall
        
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
    
    def _makeNotNullConditionals(self, cols: dict):
        return ' and '.join(("%s is not null" % (i[0]) for i in cols))
    
    def _makeSelectStatement(self,
                             tablename: str,
                             columnNames: list,
                             conditions: list=None):
        columns = ','.join(columnNames) if isinstance(columnNames, list) else columnNames
        conditions = [conditions] if isinstance(conditions, str) else conditions # Turn into a list if supplied as a single string
        conditions = ' where ' + ' and '.join(conditions) if isinstance(conditions, list) else ''
        stmt = "select %s from %s%s" % (
                columns,
                tablename,
                conditions
            )
        return stmt
    
    def _makeInsertStatement(self, tablename: str, fmt: dict, orReplace: bool=False):
        stmt = "insert%s into %s values(%s)" % (
                " or replace" if orReplace else '',
                tablename,
                self._makeQuestionMarks(fmt)
            )
        return stmt