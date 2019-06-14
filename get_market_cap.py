# coding=utf-8

from bs4 import BeautifulSoup
import urllib2
import json
from datetime import datetime
import numpy as np
import time
import xutils

# half decay as 3 days
CAP_ALPHA = -0.23104903333333335
# half decay as 7 days
VOL_ALPHA = -0.09902102579427789

def AdjustedData(lst, alpha):
    # 1 months data is enough
    total = 0.0
    bottom = 0.0
    for i in range(len(lst)):
        total += np.e ** (alpha * i) * lst[i]
        bottom += np.e ** (alpha * i)

    return total / bottom


url = "https://coinmarketcap.com/"
content = urllib2.urlopen(url).read()
soup = BeautifulSoup(content, "html.parser")

# coins = []
# for a in soup.find_all('a', {'class': 'currency-name-container'}, href=True):
#     link = a['href']
#     if link.startswith('/currencies/'):
#         # print link
#         coins.append(link.replace('currencies', '').replace('/', ''))

# coins = coins[:30]

coints = ['bitcoin', 'ethereum', 'litecoin', 'eos', 'binance-coin']

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

    cap_lst = []
    volume_lst = []
    max_dt = None
    for row in data:
        dt = datetime.strptime(row[0], '%b %d, %Y').date()
        if not max_dt or max_dt < dt:
            max_dt = dt
        if row[6] == '-':
            cap = 0.0
        else:
            cap = float(row[6].replace(',', ''))
        if row[5] == '-':
            volume = 0.0
        else:
            volume = float(row[5].replace(',', ''))
        cap_lst.append(cap)
        volume_lst.append(volume)
    cap = AdjustedData(cap_lst, CAP_ALPHA)
    volume = AdjustedData(volume_lst, VOL_ALPHA)
    print code, max_dt, cap, volume
    upsert_sql = xutils.buildUpsertOnDuplicateSql('coin_cap', ['code', 'date', 'cap', 'volume'])

    cursor.execute(upsert_sql, (code, max_dt, cap, volume) * 2)

conn.commit()