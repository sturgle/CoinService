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


def get_order_param(client, pair_symbol):
    trades = client.get_recent_trades(symbol=pair_symbol)

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

    lower = float(np.max([x, a]))
    upper = float(np.min([y, b]))
    med = float(np.median(arr))
    gap = (last_tm - first_tm).seconds / np.sqrt(len(lst))
    return [med, lower, upper, gap]


def slowly_sell(client, pair_symbol, min_notional, round_size, percent=1):
    if pair_symbol.endswith('USDT'):
        asset_symbol = pair_symbol[:-4]
    elif pair_symbol.endswith('BTC'):
        asset_symbol = pair_symbol[:-3]
    info = client.get_asset_balance(asset=asset_symbol)
    balance = float(info['free'])
    balance = balance * percent
    print balance

    # sell
    total_gap = 0.0
    flag = True
    while flag:
        [med, lower, upper, gap] = get_order_param(client, pair_symbol)
        print [med, lower, upper, gap]
        prices = client.get_all_tickers()
        for item in prices:
            if item['symbol'] == pair_symbol:
                price = str(item['price'])
        print price
        # 最后部分一起
        if balance * float(price) < min_notional:
            print 'TOO SMALL BALANCE'
            flag = False
            break
        if balance - med < lower:
            print 'LAST BALANCE'
            amount = balance
            flag = False
        else:
            amount = med
        amount = round(amount, round_size) - 0.1 ** round_size

        order = client.order_limit_sell(
            symbol=pair_symbol,
            quantity=amount,
            price=price)

        print 'ORDER', pair_symbol, amount, price
        balance = balance - amount
        time.sleep(gap)
        total_gap += gap

    print 'TOTAL WAIT TIME:', total_gap


# 从pair_symbol可以看出以什么计价（后缀），那么前面的就是quantity的数量单位
def slowly_buy(client, pair_symbol, min_notional, round_size, percent=1):
    if pair_symbol.endswith('USDT'):
        asset_symbol = 'USDT'
    elif pair_symbol.endswith('BTC'):
        asset_symbol = 'BTC'
    info = client.get_asset_balance(asset=asset_symbol)
    balance = float(info['free'])
    balance = balance * percent
    print 'BALANCE:', balance, asset_symbol

    total_gap = 0.0
    flag = True
    while flag:
        [med, lower, upper, gap] = get_order_param(client, pair_symbol)
        print [med, lower, upper, gap]
        prices = client.get_all_tickers()
        for item in prices:
            if item['symbol'] == pair_symbol:
                price = str(item['price'])
        print 'PRICE:', price
        # 最后部分一起
        # balance是以基础asset计价的
        # balance比MIN_NOTIONAL小的话，会出错
        if balance < min_notional:
            print 'TOO SMALL BALANCE'
            flag = False
            break
        if balance / float(price) - med < lower:
            print 'LAST BALANCE'
            amount = balance / float(price)
            flag = False
        else:
            amount = med

        print 'AMOUNT:', amount
        amount = round(amount, round_size) - 0.1 ** round_size

        print 'TRY:', pair_symbol, amount, amount * float(price), price

        # amount: 买多少个单位
        order = client.order_limit_buy(
            symbol=pair_symbol,
            quantity=amount,
            price=price)

        print 'ORDER:', pair_symbol, amount, amount * float(price), price
        balance = balance - amount * float(price)
        time.sleep(gap)
        total_gap += gap

    print 'TOTAL WAITING SECONDS:', total_gap



if __name__ == "__main__":
    config = xutils.getLocalConfigJson()
    api_key = config['api_key']
    api_secret = config['api_secret']

    conn = xutils.getLocalConn()

    sql = "select date, pick from coin_pick order by date desc limit 2"
    df = pd.read_sql(sql, con=conn)
    current = str(df.iloc[0]['pick'])
    prev = str(df.iloc[1]['pick'])

    if current == prev:
        # do nothing
        print 'same position, do nothing'
        exit()

    # we need to execute orders
    client = Client(api_key, api_secret)

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

    if prev == 'None' and current == 'BTC':
        pair_symbol = 'BTCUSDT'
        order_type = 'BUY'
    if prev == 'None' and current == 'ETH':
        pair_symbol = 'ETHUSDT'
        order_type = 'BUY'
    if prev == 'BTC' and current == 'ETH':
        pair_symbol = 'ETHBTC'
        order_type = 'BUY'
    if prev == 'ETH' and current == 'BTC':
        pair_symbol = 'ETHBTC'
        order_type = 'SELL'
    if prev == 'BTC' and current == 'None':
        pair_symbol = 'BTCUSDT'
        order_type = 'SELL'
    if prev == 'ETH' and current == 'None':
        pair_symbol = 'ETHUSDT'
        order_type = 'SELL'


    # about buy/sell
    precision = round(np.log(lot_size_dict[pair_symbol])/np.log(10))
    if precision >= 0:
        round_size = 0
    else:
        round_size = int(-precision)

    min_notional = min_notional_dict[pair_symbol]

    orders = client.get_open_orders(symbol=pair_symbol)
    print orders

    for order in orders:
        cancel_res = client.cancel_order(
                    symbol=pair_symbol,
                    orderId=order['orderId'])

    if order_type == 'BUY':
        slowly_buy(client, pair_symbol, min_notional, round_size, 1)
    elif order_type == 'SELL':
        slowly_sell(client, pair_symbol, min_notional, round_size, 1)
