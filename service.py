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

lag_days = 60


@app.route('/')
def hello():
    return render_template('main.html')


@app.route("/mmtm7_data.json", methods=['GET'])
def get_mmtm7_data():
    return get_mmtm_data(7)


@app.route("/mmtm15_data.json", methods=['GET'])
def get_mmtm15_data():
    return get_mmtm_data(15)


@app.route("/mmtm30_data.json", methods=['GET'])
def get_mmtm30_data():
    return get_mmtm_data(30)


def get_mmtm_data(gap):
    field = 'mmtm_' + str(gap)
    return get_field_data(field)


@app.route("/dd_data.json", methods=['GET'])
def get_dd_data():
    return get_field_data('down_std_60')


@app.route("/rsi_data.json", methods=['GET'])
def get_rsi_data():
    return get_field_data('rsi_15')


def get_field_data(field):
    conn = pool.connection();
    try:
        codes = ['BTC', 'LTC', 'IOT']
        lst = []
        today = datetime.now().date()

        for code in codes:
            sql = "select date, " + field + " from coin_close where code = %(code)s and date >= %(s_dt)s and date <= %(e_dt)s order by date"
            df = pd.read_sql(sql, con=conn, params={'code':code, 's_dt':today - relativedelta(days=lag_days), 'e_dt': today - relativedelta(days=0)})

            df = df.set_index('date')
            df.rename(columns={field: code}, inplace=True)
            lst.append(df[code])

        df = pd.concat(lst, join='inner', axis=1)
        dt_lst = []
        btc_lst = []
        ltc_lst = []
        iot_lst = []
        for index, row in df.iterrows():
            dt_lst.append(index)
            btc_lst.append(row['BTC'])
            ltc_lst.append(row['LTC'])
            iot_lst.append(row['IOT'])
        res = {'dt_lst': dt_lst, 'btc_lst': btc_lst, 'ltc_lst': ltc_lst, 'iot_lst': iot_lst}
        return jsonify(res)
    except Exception as ex:
        print (type(ex), ex)
    finally:
        if conn is not None:
            conn.close()


@app.route("/pick_data.json", methods=['GET'])
def get_pick_date():
    conn = pool.connection();
    try:
        codes = ['BTC', 'LTC', 'IOT']
        today = datetime.now().date()
        sql = "select date, pick from coin_pick where date >= %(s_dt)s and date <= %(e_dt)s order by date"
        df = pd.read_sql(sql, con=conn, params={'s_dt':today - relativedelta(days=lag_days), 'e_dt': today - relativedelta(days=0)})
        df = df.set_index('date')
        dt_lst = []
        btc_lst = []
        ltc_lst = []
        iot_lst = []

        for index, row in df.iterrows():
            dt_lst.append(index)
            btc_x = 0
            ltc_x = 0
            EOS_x = 0
            if row['pick'] == 'BTC':
                btc_x = 1
            if row['pick'] == 'LTC':
                ltc_x = 1
            if row['pick'] == 'IOT':
                EOS_x = 1
            btc_lst.append(btc_x)
            ltc_lst.append(ltc_x)
            iot_lst.append(EOS_x)

        res = {'dt_lst': dt_lst, 'btc_lst': btc_lst, 'ltc_lst': ltc_lst, 'iot_lst': iot_lst}
        return jsonify(res)
    except Exception as ex:
        print (type(ex), ex)
    finally:
        if conn is not None:
            conn.close()

@app.route("/bullbear_data.json", methods=['GET'])
def get_bb_date():
    conn = pool.connection();
    try:
        today = datetime.now().date()
        sql = "select date, bull from coin_pick where date >= %(dt)s order by date"
        df = pd.read_sql(sql, con=conn, params={'dt':today - relativedelta(days=lag_days)})
        df = df.set_index('date')
        dt_lst = []
        bb_lst = []

        for index, row in df.iterrows():
            dt_lst.append(index)
            bb_lst.append(row['bull'])

        res = {'dt_lst': dt_lst, 'bb_lst': bb_lst}
        return jsonify(res)
    except Exception as ex:
        print (type(ex), ex)
    finally:
        if conn is not None:
            conn.close()

