# -*- coding: utf-8 -*-
import quandl
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
from fake_useragent import UserAgent
import xutils


def downsideDeviation(s):
    return np.sqrt(np.sum((s.where(s < 0)) ** 2) / len(s)) * np.sqrt(252.0)


if __name__ == "__main__":
    config = xutils.getLocalConfigJson()

    DD_LAG = 45

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    codes = ['BTC', 'LTC', 'ETH']

    quandl.ApiConfig.api_key = config['key']


    for code in codes:
        print ('Get Close', code)
        url = 'https://k.sosobtc.com/data/period?symbol=huobi' + code.lower() + 'cny&step=86400'
        ua = UserAgent()
        headers = {'User-Agent': ua.chrome}
        page_src = requests.get(url, headers=headers).text

        json_data = json.loads(page_src)
        

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close'])

        for item in json_data:
            dt = datetime.fromtimestamp(float(item[0])).date()
            close = float(item[4])
            cursor.execute(upsert_sql, (code, dt, close) * 2)
        
        conn.commit()
        time.sleep(1)

    s_lst = []
    for code in codes:
        sql = "select date, close from coin_close where code = %(code)s order by date"
        df = pd.read_sql(sql, con=conn, params={'code':code})
        df = df.set_index('date')
        s_lst.append(df['close'])

        df['mmtm_7'] = np.log(3 * df['close'] / (df['close'].shift(6) + df['close'].shift(7) + df['close'].shift(8)))
        df['mmtm_7'] = df['mmtm_7'].fillna(0)
        df['mmtm_15'] = np.log(3 * df['close'] / (df['close'].shift(14) + df['close'].shift(15) + df['close'].shift(16)))
        df['mmtm_15'] = df['mmtm_15'].fillna(0)
        df['mmtm_30'] = np.log(3 * df['close'] / (df['close'].shift(29) + df['close'].shift(30) + df['close'].shift(31)))
        df['mmtm_30'] = df['mmtm_30'].fillna(0)

        df['rsi_15'] = talib.RSI(df['close'].values, 15)
        df['rsi_15'] = df['rsi_15'].fillna(0)

        df['down_std_60'] = 0.0
        s = np.log(df['close'] / df['close'].shift(1))
        for i in range(DD_LAG, len(s)):
            tmp_s = s[i - DD_LAG + 1: i + 1]
            dd = downsideDeviation(tmp_s)
            df.loc[s.index[i], 'down_std_60'] = dd

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close', 'mmtm_7', 'mmtm_15', 'mmtm_30', 'rsi_15', 'down_std_60'])

        # df = df.tail(50)

        for index, row in df.iterrows():
            cursor.execute(upsert_sql, (code, index, float(row['close']), float(row['mmtm_7']), float(row['mmtm_15']), float(row['mmtm_30']), float(row['rsi_15']), float(row['down_std_60'])) * 2)

    conn.commit()

    # 选择今日币种
    df = pd.concat(s_lst, axis=1)
    df = df.fillna(method='ffill')
    df.columns = codes

    df['masig'] = 0
    df['cnt'] = 0

    for code in codes:
        df[code + 'mmtm7'] = np.log(df[code] / (df[code].shift(6) + df[code].shift(7) + df[code].shift(8)) * 3)
        df[code + 'mmtm30'] = np.log(df[code] / (df[code].shift(29) + df[code].shift(30) + df[code].shift(31)) * 3)
        # df[code + 'bbi'] = (pd.rolling_mean(df[code], 7, 7) + pd.rolling_mean(df[code], 15, 15) + pd.rolling_mean(df[code], 30, 30) + pd.rolling_mean(df[code], 60, 60)) / 4
        df[code + 'mmtm7'] = df[code + 'mmtm7'].fillna(0)
        df[code + 'mmtm30'] = df[code + 'mmtm30'].fillna(0)
        # df[code + 'bbi'] = df[code + 'bbi'].fillna(0)
        df[code + 'rsi'] = talib.RSI(df[code].values, 15)
        df[code + 'rsi'] = df[code + 'rsi'].fillna(0)

        df[code + 'ma'] = pd.rolling_mean(df[code], 30, 30)
        df[code + 'ma'] = df[code + 'ma'].fillna(0)

        df[code + 'xma'] = pd.rolling_mean(df[code], 180, 180)
        df[code + 'xma'] = df[code + 'xma'].fillna(0)

        df[code + 'mmtm1'] = np.log(df[code] / df[code].shift(1))

        s = np.log(df[code] / df[code].shift(1))
    
        df[code + 'dd'] = 0.0
        for i in range(DD_LAG, len(s)):
            tmp_s = s[i - DD_LAG + 1: i + 1]
            dd = downsideDeviation(tmp_s)
            df.loc[s.index[i], code + 'dd'] = dd

            if df.iloc[i][code] > df.iloc[i][code + 'xma']:
                df.loc[s.index[i], 'masig'] = df.loc[s.index[i], 'masig'] + 1

            if df.iloc[i][code] >= 0:
                df.loc[s.index[i], 'cnt'] = df.loc[s.index[i], 'cnt'] + 1

    sig = 0
    df['sig'] = 0
    for index, row in df.iterrows():
        if row['cnt'] == row['masig']:
            sig = 1
        elif row['masig'] == 0:
            sig = 0
        df.loc[index, 'sig'] = sig

    last_pick = None
    cnt = 0
    dd_bar = 0.75
    rsi_bar = 90
    for index, row in df.iterrows():
        pick = None
        mmtm7_lst = []
        mmtm30_lst = []
        for code in codes:
            stoploss_bar = -0.15
            if row[code + 'mmtm1'] < stoploss_bar:
                continue

            if row[code + 'mmtm7'] > 0 and row[code + 'dd'] < dd_bar and row[code + 'rsi'] < rsi_bar:
                mmtm7_lst.append(row[code + 'mmtm7'])
            if row[code + 'mmtm7'] > 0 and row[code + 'mmtm30'] > 0 and row[code + 'dd'] < dd_bar and row[code + 'rsi'] < rsi_bar:
                mmtm30_lst.append(row[code + 'mmtm30'])
        if len(mmtm7_lst) == 0:
            pass
        elif last_pick is None:
            if len(mmtm30_lst) != 0:
                for code in codes:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst) and row[code] >= row[code + 'ma']:
                        pick = code
                        break
        elif row[last_pick + 'mmtm7'] < 0:
            if len(mmtm30_lst) != 0:
                for code in codes:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst) and row[code] >= row[code + 'ma']:
                        pick = code
                        break
        else:
            pick = last_pick
            # 试一下择强
            if len(mmtm7_lst) != 0 and len(mmtm30_lst) != 0:
                pick_7 = None
                pick_30 = None
                for code in codes:
                    if row[code + 'mmtm7'] == np.max(mmtm7_lst):
                        pick_7 = code
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst):
                        pick_30 = code

                if pick_7 == pick_30  and row[code] >= row[pick_7 + 'ma']:
                    pick = pick_7
            

        if pick == last_pick:
            pass
        else:
            cnt += 1
        df.loc[index, 'pick'] = str(pick)
        last_pick = pick

    upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_pick', ['date', 'pick', 'bull'])

    # df = df.tail(50)

    for index, row in df.iterrows():
        cursor.execute(upsert_sql, (index, row['pick'], row['sig']) * 2)

    conn.commit()

    cursor.close()
    conn.close()