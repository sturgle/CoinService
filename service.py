# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify
from flask import render_template
from flask import abort, redirect, url_for
import pandas as pd
import numpy as np
import pymysql
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from DBUtils.PooledDB import PooledDB
import os
import xutils


config = xutils.getLocalConfigJson()

pool = PooledDB(creator=pymysql,
            mincached=1,
            maxcached=5,
            host=config['host'],
            port=config['port'],
            user=config['user'],
            passwd=config['password'],
            db=config['db'],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor)

app = Flask(__name__)


@app.route('/')
def hello():
    return render_template('main.html')


@app.route("/data.json", methods=['GET'])
def get_mmtm_data():
    conn = pool.connection();
    try:
        codes = ['BTC', 'LTC', 'ETH']
        lst = []
        today = datetime.now().date()

        for code in codes:
            sql = "select date, mmtm_7 from coin_close where code = %(code)s and date >= %(dt)s"
            df = pd.read_sql(sql, con=conn, params={'code':code, 'dt':today - relativedelta(months=1)})
            df = df.set_index('date')
            df.rename(columns={'mmtm_7': code}, inplace=True)
            lst.append(df[code])

        df = pd.concat(lst, join='inner', axis=1)
        dt_lst = []
        btc_lst = []
        ltc_lst = []
        eth_lst = []
        for index, row in df.iterrows():
            dt_lst.append(index)
            btc_lst.append(row['BTC'])
            ltc_lst.append(row['LTC'])
            eth_lst.append(row['ETH'])
        res = {'dt_lst': dt_lst, 'btc_lst': btc_lst, 'ltc_lst': ltc_lst, 'eth_lst': eth_lst}
        return jsonify(res)
    except Exception as ex:
        print (type(ex), ex)
    finally:
        if conn is not None:
            conn.close()