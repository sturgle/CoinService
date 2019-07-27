# -*- coding: utf-8 -*-
# import quandl
import pandas as pd
import numpy as np
import pymysql
import requests
import json
# import bs4
import time
import sys
from datetime import datetime
import talib
from binance.client import Client
import xutils


if __name__ == "__main__":
    config = xutils.getLocalConfigJson()
    api_key = config['api_key']
    api_secret = config['api_secret']

    DD_LAG = 45

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    client = Client(api_key, api_secret)

    # 熊市只从BTC和ETH里面选择
    codes = {
        'BTC': 'BTCUSDT',
        'ETH': 'ETHUSDT',
        # 'LTC': 'LTCUSDT'
    }

    for code in codes:
        print ('Get Close', codes[code])

        candles = client.get_klines(symbol=codes[code], interval=Client.KLINE_INTERVAL_1DAY)

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close'])

        # discard last one
        for c in candles[:-1]:
            dt = datetime.fromtimestamp(c[0]/1000).date()
            cursor.execute(upsert_sql, (code, dt, float(c[4])) * 2)
        conn.commit()
        time.sleep(5)

    s_lst = []
    for code in codes:
        sql = "select date, close from coin_close where code = %(code)s order by date"
        df = pd.read_sql(sql, con=conn, params={'code':code})
        df = df.set_index('date')
        s_lst.append(df['close'])

        df['mmtm30'] = np.log(3 * df['close'] / (df['close'].shift(29) + df['close'].shift(30) + df['close'].shift(31)))
        df['mmtm30'] = df['mmtm30'].fillna(0)

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close'])

        df = df.tail(30)

        for index, row in df.iterrows():
            cursor.execute(upsert_sql, (code, index, float(row['close'])) * 2)

    conn.commit()


    # 选择今日币种
    df = pd.concat(s_lst, axis=1)
    df = df.fillna(method='ffill')
    df.columns = codes

    df['masig'] = 0
    df['cnt'] = 0

    for code in codes:
        df[code + '_mmtm'] = np.log(df[code] / (df[code].shift(29) + df[code].shift(30) + df[code].shift(31)) * 3)
        df[code + '_mmtm'] = df[code + '_mmtm'].fillna(0)

        df[code + '_xma'] = df[code].rolling(120).mean()
        df[code + '_xma'] = df[code + '_xma'].fillna(0)

        for idx, row in df.iterrows():
            if row[code] > row[code + '_xma']:
                df.loc[idx, 'masig'] = df.loc[idx, 'masig'] + 1

    bull = 0
    df['bull'] = 0

    last_dt = datetime(2010, 1, 1).date()
    pick = 'None'
    for idx, row in df.iterrows():
        if (idx - last_dt).days <= 5:
            if pick == 'BTC':
                df.loc[idx, 'BTC_sig'] = 1
            else:
                df.loc[idx, 'ETH_sig'] = 1
        elif row['BTC_mmtm'] >= row['ETH_mmtm']:
            df.loc[idx, 'BTC_sig'] = 1
            if 'BTC' != pick:
                last_dt = idx
                df.loc[idx, 'cost'] = -0.01
            pick = 'BTC'
        else:
            df.loc[idx, 'ETH_sig'] = 1
            if 'ETH' != pick:
                last_dt = idx
                df.loc[idx, 'cost'] = -0.01
            pick = 'ETH'
        if bull == 0 and row['masig'] == len(codes):
            bull = 1
        elif bull == 1 and row['masig'] == 0:
            bull = 0
        
        df.loc[idx, 'bull'] = bull
        if bull == 1:
            df.loc[idx, 'pick'] = pick
        else:
            df.loc[idx, 'pick'] = 'None'

    upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_pick', ['date', 'pick', 'bull'])

    # df = df.tail(30)

    for index, row in df.iterrows():
        cursor.execute(upsert_sql, (index, row['pick'], row['bull']) * 2)

    conn.commit()

    cursor.close()
    conn.close()