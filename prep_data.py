# -*- coding: utf-8 -*-
# import quandl
import pandas as pd
import numpy as np
import pymysql
import requests
import json
import time
import sys
from datetime import datetime
import talib
import xutils


def downsideDeviation(s):
    return np.sqrt(np.sum((s.where(s < 0)) ** 2) / len(s)) * np.sqrt(252.0)


if __name__ == "__main__":
    config = xutils.getLocalConfigJson()

    DD_LAG = 45

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    codes = {
            'BTC': 'bitfinex/btcusd',
            'LTC': 'bitfinex/ltcusd',
            # 'XLM': 'poloniex/strusdt'
            'EOS': 'bitfinex/eosusd',
            # 'BTS': ['poloniex/btsbtc', 'poloniex/btcusdt']
    }

    for code in codes:
        print ('fetching', code)
        urlcode = codes[code]
        if type(urlcode) is list:
            dict_lst = []
            dt_lst = []
            for tcode in urlcode:
                dct = {}
                url = "https://api.cryptowat.ch/markets/" + tcode + "/ohlc"
                print url
                page_src = requests.get(url).text
                ticks = json.loads(page_src)
                close_lst = ticks['result']['86400']
                for item in close_lst:
                    dt = datetime.fromtimestamp(item[0]).date()
                    if dt.year >= 2017:
                        dct[dt] = float(item[4])
                    if dt not in dt_lst:
                        dt_lst.append(dt)
                dict_lst.append(dct)
                time.sleep(3)

            price_lst = []
            dt_lst = sorted(dt_lst)
            for dt in dt_lst:
                flag = True
                mul_lst = []
                for dct in dict_lst:
                    if dt not in dct:
                        flag = False
                        break
                    else:
                        mul_lst.append(dct[dt])
                if flag:
                    close = 1.0
                    for mul in mul_lst:
                        close *= mul
                    price_lst.append([dt, close])

        else:
            url = "https://api.cryptowat.ch/markets/" + urlcode + "/ohlc"
            print url
            page_src = requests.get(url).text
            ticks = json.loads(page_src)
            close_lst = ticks['result']['86400']
            price_lst = []
            for item in close_lst:
                dt = datetime.fromtimestamp(item[0]).date()
                if dt.year >= 2017:
                    price_lst.append([dt, float(item[4])])

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close'])
        for item in price_lst[:-1]:
            cursor.execute(upsert_sql, (code, item[0], float(item[1])) * 2)
        conn.commit()
        time.sleep(3)

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

        df[code + 'ema'] = talib.EMA(df[code].values, 30)
        df[code + 'ema'] = df[code + 'ema'].fillna(0)

        df[code + 'ma'] = pd.rolling_mean(df[code], 30, 30) 
        df[code + 'ma'] = df[code + 'ma'].fillna(0)

        df[code + 'xma'] = talib.EMA(df[code].values, 180)
        df[code + 'xma'] = df[code + 'xma'].fillna(0)

        df[code + 'mmtm1'] = np.log(df[code] / df[code].shift(1))

        s = np.log(df[code] / df[code].shift(1))
    
        for i in range(DD_LAG, len(s)):
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
    rsi_bar = 90

    for index, row in df.iterrows():
        class1_lst = []
        class2_lst = []
        mmtm30_lst1 = []
        mmtm30_lst2 = []
        for code in codes:
            if row[code + 'rsi'] > rsi_bar:
                pass
            elif row[code] >= row[code + 'ema'] and row[code] >= row[code + 'ma']:
                # 既在ma30上，又在mmtm7上
                if row[code + 'mmtm7'] >= 0:
                    class1_lst.append(code)
                    mmtm30_lst1.append(row[code + 'mmtm30'])
                # 只在ma30上
                else:
                    class2_lst.append(code)
                    mmtm30_lst2.append(row[code + 'mmtm30'])
        # 默认延续上次选择
        pick = last_pick
        if len(class2_lst) == 0 and len(class1_lst) == 0:
            pick = None
            # print 'EMPTY', index
            # 空仓
            pass
        elif last_pick is None:
            # 开仓，选择mmtm30最强的
            if len(class1_lst) != 0:
                for code in class1_lst:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst1) and row[code + 'mmtm30'] > 0:
                        pick = code
                        # print 'OPEN 1', index, pick
                        break
            elif len(class2_lst) != 0:
                for code in class2_lst:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst2) and row[code + 'mmtm30'] > 0:
                        pick = code
                        # print 'OPEN 2', index, pick
                        break
        elif last_pick in class1_lst:
            # 上次的选择依然强势，那么看看是否可以选择更强
            for code in class1_lst:
                if row[code + 'mmtm30'] == np.max(mmtm30_lst1) and row[code + 'mmtm7'] > row[last_pick + 'mmtm7']:
                    pick = code
                    # print 'STRONG 1 1', index, pick
                    break
        else:
            # 上次的选择降级了在class2中
            # 先从class1看是否有候选
            if len(class1_lst) != 0:
                for code in class1_lst:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst1):
                        pick = code
                        # print 'STRONG 2 1', index, pick
                        break
            # 再从class2中择强
            elif len(class2_lst) != 0:
                for code in class2_lst:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst2) and row[code + 'mmtm7'] > row[last_pick + 'mmtm7']:
                        pick = code
                        # print 'STRONG 2 2', index, pick
                        break
            
        if pick == last_pick:
            pass
        else:
            if last_pick is None:
                last_close = 1.0
            else:
                last_close = row[last_pick]
            if pick is None:
                this_close = 1.0
            else:
                this_close = row[pick]
            print (index, last_pick, last_close, '==>', pick, this_close)
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
