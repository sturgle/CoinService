#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import sys
import pandas as pd
import numpy as np
import time
import json
import pymysql


def getLocalConfigJson():
    dirname = os.path.dirname(__file__)
    filepath = os.path.join(dirname, 'config.json')

    f = open(filepath, 'r');
    config = json.load(f)
    f.close()
    return config