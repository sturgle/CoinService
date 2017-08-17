# -*- coding: utf-8 -*-
import quandl
import pandas as pd
import numpy as np
import pymysql
import talib

import xutils


def downsideDeviation(s):
    return np.sqrt(np.sum((s.where(s < 0)) ** 2) / len(s)) * np.sqrt(252.0)


if __name__ == "__main__":
    config = xutils.getLocalConfigJson()
    quandl.ApiConfig.api_key = config['key']

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    codes = ['BTC', 'LTC', 'ETH']
    s_lst = []
    for code in codes:
        df = quandl.get("BITFINEX/" + code + "USD")
        s_lst.append(df['Last'])

        df['mmtm_7'] = np.log(3 * df['Last'] / (df['Last'].shift(6) + df['Last'].shift(7) + df['Last'].shift(8)))
        df['mmtm_7'] = df['mmtm_7'].fillna(0)
        df['mmtm_15'] = np.log(3 * df['Last'] / (df['Last'].shift(14) + df['Last'].shift(15) + df['Last'].shift(16)))
        df['mmtm_15'] = df['mmtm_15'].fillna(0)
        df['mmtm_30'] = np.log(3 * df['Last'] / (df['Last'].shift(29) + df['Last'].shift(30) + df['Last'].shift(31)))
        df['mmtm_30'] = df['mmtm_30'].fillna(0)
        df['ma_125'] = talib.SMA(df['Last'].values, 125)
        df['down_std_60'] = 0.0
        s = np.log(df['Last'] / df['Last'].shift(1))
        for i in range(60, len(s)):
            tmp_s = s[i - 60: i]
            dd = downsideDeviation(tmp_s)
            df.loc[s.index[i], 'down_std_60'] = dd

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close', 'mmtm_7', 'mmtm_15', 'mmtm_30', 'ma_125', 'down_std_60'])

        df = df.tail(50)

        for index, row in df.iterrows():
            cursor.execute(upsert_sql, (code, index.date(), float(row['Last']), float(row['mmtm_7']), float(row['mmtm_15']), float(row['mmtm_30']), float(row['ma_125']), float(row['down_std_60'])) * 2)

    conn.commit()

    # 选择今日币种
    df = pd.concat(s_lst, axis=1)
    df = df.fillna(method='ffill')
    df.columns = codes

    for code in codes:
        df[code + 'mmtm7'] = np.log(df[code] / (df[code].shift(6) + df[code].shift(7) + df[code].shift(8)) * 3)
        df[code + 'mmtm30'] = np.log(df[code] / (df[code].shift(29) + df[code].shift(30) + df[code].shift(31)) * 3)
        df[code + 'bbi'] = (pd.rolling_mean(df[code], 7, 7) + pd.rolling_mean(df[code], 15, 15) + pd.rolling_mean(df[code], 30, 30) + pd.rolling_mean(df[code], 60, 60)) / 4
        df[code + 'mmtm7'] = df[code + 'mmtm7'].fillna(0)
        df[code + 'mmtm30'] = df[code + 'mmtm30'].fillna(0)
        df[code + 'bbi'] = df[code + 'bbi'].fillna(0)

    last_pick = None
    cnt = 0
    for index, row in df.iterrows():
        pick = None
        mmtm7_lst = []
        mmtm30_lst = []
        for code in codes:
            if row[code + 'mmtm7'] > 0:
                mmtm7_lst.append(row[code + 'mmtm7'])
            if row[code + 'mmtm7'] > 0 and row[code + 'mmtm30'] > 0:
                mmtm30_lst.append(row[code + 'mmtm30'])
        if len(mmtm7_lst) == 0:
            pass
        elif last_pick is None:
            if len(mmtm30_lst) != 0:
                for code in codes:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst) and row[code] >= row[code + 'bbi']:
                        pick = code
                        break
        elif row[last_pick + 'mmtm7'] < 0:
            if len(mmtm30_lst) != 0:
                for code in codes:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst):
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

                if pick_7 == pick_30:
                    pick = pick_7
            

        if pick == last_pick:
            pass
        else:
            cnt += 1
        df.loc[index, 'pick'] = str(pick)
        last_pick = pick

    upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_pick', ['date', 'pick'])

    df = df.tail(50)

    for index, row in df.iterrows():
        cursor.execute(upsert_sql, (index.date(), row['pick']) * 2)

    conn.commit()

    cursor.close()
    conn.close()