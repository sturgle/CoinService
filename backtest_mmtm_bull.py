# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import pymysql
import requests
import json
import bs4
import time
import sys
from datetime import datetime
import talib
import xutils


from binance.client import Client
import json
from datetime import datetime


if __name__ == "__main__":

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    codes = {
        'BTC': 'BTCUSDT',
        'ETH': 'ETHUSDT',
    }


    s_lst = []
    for code in codes:
        sql = "select date, close from coin_close where code = %(code)s order by date"
        df = pd.read_sql(sql, con=conn, params={'code':code})
        df = df.set_index('date')
        s_lst.append(df['close'])

    cursor.close()
    conn.close()

    # 选择今日币种
    df = pd.concat(s_lst, axis=1)
    df = df.fillna(method='ffill')
    df.columns = codes
    df['masig'] = 0

    for code in codes:
        df[code + '_mmtm'] = np.log(df[code] / (df[code].shift(29) + df[code].shift(30) + df[code].shift(31)) * 3)
        df[code + '_mmtm'] = df[code + '_mmtm'].fillna(0)
        df[code + '_sig'] = 0
        df[code + '_logrtn'] = np.log(df[code] / df[code].shift(1))
        df[code + '_xma'] = df[code].rolling(100).mean()
        df[code + '_xma'] = df[code + '_xma'].fillna(0)

        for idx, row in df.iterrows():
            if row[code] > row[code + '_xma']:
                df.loc[idx, 'masig'] = df.loc[idx, 'masig'] + 1

    df = df.loc[datetime(2016, 1, 1).date(): ]
    df['cost'] = 0.0
    df['bull'] = 0
    sig = 'None'
    bull = 0
    last_dt = datetime(2010, 1, 1).date()
    for idx, row in df.iterrows():
        if (idx - last_dt).days <= 5:
            if sig == 'BTC':
                df.loc[idx, 'BTC_sig'] = 1
            else:
                df.loc[idx, 'ETH_sig'] = 1
        elif row['BTC_mmtm'] >= row['ETH_mmtm']:
            df.loc[idx, 'BTC_sig'] = 1
            if 'BTC' != sig:
                last_dt = idx
                df.loc[idx, 'cost'] = -0.01
            sig = 'BTC'
        else:
            df.loc[idx, 'ETH_sig'] = 1
            if 'ETH' != sig:
                df.loc[idx, 'cost'] = -0.01
            sig = 'ETH'
        if bull == 0 and row['masig'] == 2:
            bull = 1
        elif bull == 1 and row['masig'] == 0:
            bull = 0
        df.loc[idx, 'bull'] = bull

    for code in codes:
        df[code + '_sig'] = df[code + '_sig'].shift(1)

    df['logrtn'] = df['ETH_sig'] * df['ETH_logrtn'] + df['BTC_sig'] * df['BTC_logrtn']
    df['logrtn2'] = df['ETH_sig'] * df['ETH_logrtn'] + df['BTC_sig'] * df['BTC_logrtn'] + df['cost']
    df['logrtn3'] = df['logrtn2'] * df['bull']

    df['cumsum'] = df['logrtn'].cumsum()
    df['cumsum2'] = df['logrtn2'].cumsum()
    df['cumsum3'] = df['logrtn3'].cumsum()

    df['val'] = np.exp(df['cumsum'])
    df['val2'] = np.exp(df['cumsum2'])
    df['val3'] = np.exp(df['cumsum3'])
    
    print df.head()
    print df.tail()

    df.to_excel('x.xlsx')
