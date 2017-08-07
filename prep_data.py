# -*- coding: utf-8 -*-
import quandl
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
    for code in codes:
        df = quandl.get("BITFINEX/" + code + "USD")

        df['mmtm_7'] = np.log(3 * df['Last'] / (df['Last'].shift(6) + df['Last'].shift(7) + df['Last'].shift(8)))
        df['mmtm_7'] = df['mmtm_7'].fillna(0)
        df['mmtm_15'] = np.log(3 * df['Last'] / (df['Last'].shift(14) + df['Last'].shift(15) + df['Last'].shift(16)))
        df['mmtm_15'] = df['mmtm_15'].fillna(0)
        df['ma_125'] = talib.SMA(df['Last'].values, 125)
        df['down_std_60'] = 0.0
        s = np.log(df['Last'] / df['Last'].shift(1))
        for i in range(60, len(s)):
            tmp_s = s[i - 60: i]
            dd = downsideDeviation(tmp_s)
            df.loc[s.index[i], 'down_std_60'] = dd

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_close', ['code', 'date', 'close', 'mmtm_7', 'mmtm_15', 'ma_125', 'down_std_60'])

        df = df.tail(15)

        for index, row in df.iterrows():
            cursor.execute(upsert_sql, (code, index.date(), float(row['Last']), float(row['mmtm_7']), float(row['mmtm_15']), float(row['ma_125']), float(row['down_std_60'])) * 2)

    conn.commit()
    cursor.close()
    conn.close()