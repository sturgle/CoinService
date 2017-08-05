# -*- coding: utf-8 -*-
import quandl
import numpy as np
import pymysql
import talib

import xutils


if __name__ == "__main__":
    config = xutils.getLocalConfigJson()
    quandl.ApiConfig.api_key = config['key']

    conn = xutils.getLocalConn()
    cursor = conn.cursor()

    codes = ['BTC', 'LTC', 'ETH']
    for code in codes:
        df = quandl.get("BITFINEX/" + code + "USD")

        df['mmtm_7'] = np.log(df['Last'] / df['Last'].shift(7))
        df['mmtm_7'] = df['mmtm_7'].fillna(0)
        df['mmtm_15'] = np.log(df['Last'] / df['Last'].shift(15))
        df['mmtm_15'] = df['mmtm_15'].fillna(0)
        df['ma_125'] = talib.SMA(df['Last'].values, 125)

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close', 'mmtm_7', 'mmtm_15', 'ma_125'])

        df = df.tail(15)

        for index, row in df.iterrows():
            cursor.execute(upsert_sql, (code, index.date(), float(row['Last']), float(row['mmtm_7']), float(row['mmtm_15']), float(row['ma_125'])) * 2)

    conn.commit()
    cursor.close()
    conn.close()