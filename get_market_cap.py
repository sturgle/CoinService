# coding=utf-8

from bs4 import BeautifulSoup
import urllib2
import json
from datetime import datetime
import numpy as np
import time
import xutils

def AdjustedMarketCap(lst):
    # half decay as 3 days
    ALPHA = -0.23104903333333335

    # 1 months data is enough
    total = 0.0
    bottom = 0.0
    for i in range(len(lst)):
        total += np.e ** (ALPHA * i) * lst[i]
        bottom += np.e ** (ALPHA * i)

    return total / bottom


url = "https://coinmarketcap.com/"
content = urllib2.urlopen(url).read()
soup = BeautifulSoup(content, "html.parser")

coins = []
for a in soup.find_all('a', {'class': 'currency-name-container'}, href=True):
    link = a['href']
    if link.startswith('/currencies/'):
        # print link
        coins.append(link.replace('currencies', '').replace('/', ''))

coins = coins[:30]

config = xutils.getLocalConfigJson()
conn = xutils.getLocalConn()
cursor = conn.cursor()

for code in coins:
    time.sleep(1)
    url = 'https://coinmarketcap.com/currencies/' + code + '/historical-data/'
    content = urllib2.urlopen(url).read()
    soup = BeautifulSoup(content, "html.parser")
    div = soup.find('div', {'id':'historical-data'})
    table = div.find('table')
    table_body = table.find('tbody')
    data = []
    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele]) # Get rid of empty values

    lst = []
    max_dt = None
    for row in data:
        dt = datetime.strptime(row[0], '%b %d, %Y').date()
        if not max_dt or max_dt < dt:
            max_dt = dt
        if row[6] == '-':
            cap = 0.0
        else:
            cap = float(row[6].replace(',', ''))
        lst.append(cap)
    cap = AdjustedMarketCap(lst)
    print code, max_dt, cap
    upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_cap', ['code', 'date', 'cap'])

    cursor.execute(upsert_sql, (code, max_dt, cap) * 2)

conn.commit()