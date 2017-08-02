#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import sys
import pandas as pd
import numpy as np
import time
import json
import pymysql


def getLocalConfigJson():
    dirname = os.path.dirname(__file__)
    filepath = os.path.join(dirname, 'config.json')

    f = open(filepath, 'r');
    config = json.load(f)
    f.close()
    return config


def getLocalConn():
    config = getLocalConfigJson()
    conn = pymysql.connect(host=config['host'],
                                     user=config['user'],
                                     password=config['password'],
                                     db=config['db'],
                                     cursorclass=pymysql.cursors.DictCursor)
    return conn


def buildUpsertOnDuplicateSql(table, field_lst):
    sql = 'insert into '
    sql += table
    sql += ' '
    sql += '(`'
    sql += '`, `'.join(field_lst)
    sql += '`)'
    sql += ' values '
    sql += '('
    sql += ', '.join(['%s'] * len(field_lst))
    sql += ')'
    sql += ' on duplicate key update '
    sql += ', '.join(x + ' = %s' for x in field_lst)
    return sql