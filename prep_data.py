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

import xutils


def downsideDeviation(s):
    return np.sqrt(np.sum((s.where(s < 0)) ** 2) / len(s)) * np.sqrt(252.0)


if __name__ == "__main__":
    config = xutils.getLocalConfigJson()

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    codes = {
        'BTC': 'bitcoin',
        'LTC': 'litecoin',
        'ETH': 'ethereum'
    }


    for code in codes:
        print ('Get Close', code)
        url_code = codes[code]
        url = 'https://coinmarketcap.com/currencies/' + url_code + '/historical-data/?start=20130428&end=20170818'

        page_src = requests.get(url).text
        soup = bs4.BeautifulSoup(page_src, 'html.parser')
        div = soup.find('div', {'id':'historical-data'})
        table = div.find('table')
        table_body = table.find('tbody')
        data = []
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele]) # Get rid of empty values

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close'])

        for row in data:
            dt = datetime.strptime(row[0], '%b %d, %Y')
            cursor.execute(upsert_sql, (code, dt.date(), float(row[4])) * 2)
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
        df['down_std_60'] = 0.0
        s = np.log(df['close'] / df['close'].shift(1))
        for i in range(60, len(s)):
            tmp_s = s[i - 60 + 1: i + 1]
            dd = downsideDeviation(tmp_s)
            df.loc[s.index[i], 'down_std_60'] = dd

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close', 'mmtm_7', 'mmtm_15', 'mmtm_30', 'down_std_60'])

        df = df.tail(50)

        for index, row in df.iterrows():
            cursor.execute(upsert_sql, (code, index, float(row['close']), float(row['mmtm_7']), float(row['mmtm_15']), float(row['mmtm_30']), float(row['down_std_60'])) * 2)

    conn.commit()

    # 选择今日币种
    df = pd.concat(s_lst, axis=1)
    df = df.fillna(method='ffill')
    df.columns = codes

    for code in codes:
        df[code + 'mmtm7'] = np.log(df[code] / (df[code].shift(6) + df[code].shift(7) + df[code].shift(8)) * 3)
        df[code + 'mmtm30'] = np.log(df[code] / (df[code].shift(29) + df[code].shift(30) + df[code].shift(31)) * 3)
        # df[code + 'bbi'] = (pd.rolling_mean(df[code], 7, 7) + pd.rolling_mean(df[code], 15, 15) + pd.rolling_mean(df[code], 30, 30) + pd.rolling_mean(df[code], 60, 60)) / 4
        df[code + 'mmtm7'] = df[code + 'mmtm7'].fillna(0)
        df[code + 'mmtm30'] = df[code + 'mmtm30'].fillna(0)
        # df[code + 'bbi'] = df[code + 'bbi'].fillna(0)

        s = np.log(df[code] / df[code].shift(1))
    
        df[code + 'dd60'] = 0.0
        for i in range(60, len(s)):
            tmp_s = s[i - 60 + 1: i + 1]
            dd = downsideDeviation(tmp_s)
            df.loc[s.index[i], code + 'dd60'] = dd

    last_pick = None
    cnt = 0
    for index, row in df.iterrows():
        pick = None
        mmtm7_lst = []
        mmtm30_lst = []
        for code in codes:
            if row[code + 'mmtm7'] > 0 and row[code + 'dd60'] < 0.75:
                mmtm7_lst.append(row[code + 'mmtm7'])
            if row[code + 'mmtm7'] > 0 and row[code + 'mmtm30'] > 0 and row[code + 'dd60'] < 0.75:
                mmtm30_lst.append(row[code + 'mmtm30'])
        if len(mmtm7_lst) == 0:
            pass
        elif last_pick is None:
            if len(mmtm30_lst) != 0:
                for code in codes:
                    if row[code + 'mmtm30'] == np.max(mmtm30_lst):
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
        cursor.execute(upsert_sql, (index, row['pick']) * 2)

    conn.commit()

    cursor.close()
    conn.close()