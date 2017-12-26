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
        url = 'https://coinmarketcap.com/currencies/' + url_code + '/historical-data/?start=20130428&end=20171231'

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

    cursor.close()
    conn.close()

    # 选择今日币种
    df = pd.concat(s_lst, axis=1)
    df = df.fillna(method='ffill')
    df.columns = codes

    # df = df.loc[datetime(2016, 1, 1).date(): ]

    print df.head()
    print df.tail()

    for code in codes:
        df[code + 'mmtm7'] = np.log(df[code] / (df[code].shift(6) + df[code].shift(7) + df[code].shift(8)) * 3)
        df[code + 'mmtm30'] = np.log(df[code] / (df[code].shift(29) + df[code].shift(30) + df[code].shift(31)) * 3)
        df[code + 'mmtm7'] = df[code + 'mmtm7'].fillna(0)
        df[code + 'mmtm30'] = df[code + 'mmtm30'].fillna(0)
        df[code + 'rsi'] = talib.RSI(df[code].values, 15)
        df[code + 'rsi'] = df[code + 'rsi'].fillna(0)

        df[code + 'ma'] = pd.rolling_mean(df[code], 30, 30)
        df[code + 'ma'] = df[code + 'ma'].fillna(0)

        df[code + 'ma7'] = pd.rolling_mean(df[code], 7, 7)
        df[code + 'ma7'] = df[code + 'ma7'].fillna(0)

        df[code + 'mmtm1'] = np.log(df[code] / df[code].shift(1))

    last_pick = None
    cnt = 0
    rsi_bar = 90
    for index, row in df.iterrows():
        class1_lst = []
        class2_lst = []
        mmtm30_lst1 = []
        mmtm30_lst2 = []
        for code in codes:
            stoploss_bar = -0.15
            if row[code + 'mmtm1'] < stoploss_bar:
                continue
            if row[code + 'rsi'] > rsi_bar:
                continue
            # 既在ma30上，又在mmtm7上
            if row[code + 'mmtm7'] >= 0 and row[code] >= row[code + 'ma']:
                class1_lst.append(code)
                mmtm30_lst1.append(row[code + 'mmtm30'])
            # 只在ma30上
            elif row[code] >= row[code + 'ma']:
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
            print index, last_pick, last_close, '==>', pick, this_close
            cnt += 1
        df.loc[index, 'pick'] = str(pick)
        last_pick = pick

