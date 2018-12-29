# coding=utf-8

from bs4 import BeautifulSoup
import urllib2
import json
from datetime import datetime
import numpy as np
import time
import xutils

config = xutils.getLocalConfigJson()
conn = xutils.getLocalConn()
cursor = conn.cursor()

url = "https://coinmarketcap.com/historical/"
content = urllib2.urlopen(url).read()
soup = BeautifulSoup(content, "html.parser")

links = []
for a in soup.find_all('a', href=True):
    link = a['href']
    if link.startswith('/historical/') and link != '/historical/':
        links.append(link)

flag = False
for link in links:
    url = "https://coinmarketcap.com" + link
    print url
    # url = "https://coinmarketcap.com/historical/20181104/"
    dt = url.replace('https://coinmarketcap.com/historical/', '').replace('/', '')
    if dt == '20181223':
        flag = True

    if not flag:
        continue
    dt = dt[:4] + '-' + dt[4:6] + '-' + dt[6:]
    time.sleep(0.3)
    content = urllib2.urlopen(url).read()
    soup = BeautifulSoup(content, "html.parser")
    links = soup.find_all('a', {'class': 'volume'}, href=True)
    for a in links:
        href = a['href']
        coin = href.replace('/currencies/', '').replace('/#markets', '')
        volume = a['data-usd']

        upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_cap', ['code', 'date', 'volume'])
        cursor.execute(upsert_sql, (coin[:30], dt, volume) * 2)

    conn.commit()

