import pandas as pd
from datetime import timedelta
import time
import datetime

def string_to_float(string):
    if isinstance(string, str):
        return float(string)
    else:
        return string

def pandas_factory(col_names, table):
    return pd.DataFrame(table, columns=col_names)

def splitTextToTriplet(string, n):
    words = string.split()
    grouped_words = [' '.join(words[i: i + n]) for i in range(0, len(words), n)]
    return grouped_words

def convertPrice(x: str):
    return x.replace("\xa0", "")

def convertTime(x: str):
    return time.mktime(datetime.datetime.strptime(x, "%d/%m/%Y").timetuple())

def convertChange(x: str):
    x = x.split("(")[1]
    return x.split(" %")[0]

def convertSingle(x: str):
    x = x.replace("\r\n", "")
    return x.replace(" ", "")

def convertDate(x: str):
    x = x.split(" ")
    day = x[3].split("/")
    hour = x[0].split(":")
    return datetime.datetime(int(day[2]),int(day[1]), int(day[0]), int(hour[1]), int(hour[0]), 0, 0).timestamp()

SYMBOL_LIST = [
    "STB", "VIC", "SSI", "MSN", "FPT", "HAG", "KDC", "EIB", "DPM", "VNM",
    ]   

TIME_ZONE = 'US/Eastern'

def prev_weekday(adate):
    while adate.weekday() >= 5: # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate

