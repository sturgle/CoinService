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

# 'BTCUSDT'
def get_order_param(client, symb):
    trades = client.get_recent_trades(symbol=symb)

    lst = []
    first_tm = None
    last_tm = None
    for trade in trades:
        tm = datetime.fromtimestamp(trade['time']/1000)
        last_tm = tm
        if first_tm is None:
            first_tm = tm
        lst.append(float(trade['qty']))

    n = int(np.sqrt(len(lst)))
    lsts = [lst[i:i + n] for i in xrange(0, len(lst), n)]
    med_lst = []
    for l in lsts:
        arr = np.array(l)
        med_lst.append(np.median(l))

    # print med_lst
    arr = np.array(med_lst)
    a = np.mean(arr) - np.std(arr) / 2
    b = np.mean(arr) + np.std(arr) / 2
    x = np.min(arr)
    y = np.max(arr)
    # print np.max([x, a]), np.min([y, b])
    lower = float(np.max([x, a]))
    upper = float(np.min([y, b]))
    med = float(np.median(arr))
    gap = (last_tm - first_tm).seconds / np.sqrt(len(lst))
    return [med, lower, upper, gap]


def slowly_sell(client, symbol, balance, round_size):
    # sell
    total_gap = 0.0
    flag = True
    while flag:
        [med, lower, upper, gap] = get_order_param(client, symbol)
        print [med, lower, upper, gap]
        prices = client.get_all_tickers()
        for item in prices:
            if item['symbol'] == symbol:
                price = str(item['price'])
        print price
        # 最后的就全部吧
        if balance - med < lower:
            print 'LAST'
            amount = balance
            flag = False
        else:
            amount = med
        amount = round(amount, round_size) - 0.1 ** round_size

        order = client.order_limit_sell(
            symbol=symbol,
            quantity=amount,
            price=price)

        print 'ORDER', symbol, amount, price
        balance = balance - amount
        time.sleep(gap)
        total_gap += gap

    print 'TOTAL WAIT TIME:', total_gap

if __name__ == "__main__":
    config = xutils.getLocalConfigJson()
    api_key = config['api_key']
    api_secret = config['api_secret']

    client = Client(api_key, api_secret)

    prices = client.get_all_tickers()

    price_dict = {}

    for item in prices:
        price_dict[item['symbol']] = float(item['price'])

    min_notional_dict = {}
    lot_size_dict = {}
    exchange_info = client.get_exchange_info()
    for s in exchange_info['symbols']:
        filters = s['filters']
        for filter in filters:
            if filter['filterType'] == 'MIN_NOTIONAL':
                min_notional_dict[s['symbol']] = float(filter['minNotional'])
            if filter['filterType'] == 'LOT_SIZE':
                lot_size_dict[s['symbol']] = float(filter['minQty'])

    
    asset_symbol = 'DASH'
    symbol = asset_symbol + 'BTC'#'USDT'
    info = client.get_asset_balance(asset=asset_symbol)
    balance = float(info['free'])
    print balance

    precision = round(np.log(lot_size_dict[symbol])/np.log(10))
    if precision >= 0:
        round_size = 0
    else:
        round_size = int(-precision)

    slowly_sell(client, symbol, balance, round_size)
