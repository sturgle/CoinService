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


    # about buy/sell
    pair_symbol = 'BTCUSDT'
    precision = round(np.log(lot_size_dict[pair_symbol])/np.log(10))
    if precision >= 0:
        round_size = 0
    else:
        round_size = int(-precision)

    min_notional = min_notional_dict[pair_symbol]

    slowly_sell(client, pair_symbol, min_notional, round_size, 0.01)
    slowly_buy(client, pair_symbol, min_notional, round_size, 0.01)
