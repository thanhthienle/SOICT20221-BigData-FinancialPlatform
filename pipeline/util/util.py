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

def toFloat(x: str):
    if isinstance(x, str):
      return float("".join(x.split(',')))
    return x

def toInt(x: str):
    if isinstance(x, str):
      return int("".join(x.split(',')))
    return x

def convertToDate(x: str):
    if isinstance(x, str):
      return datetime.datetime.strptime(x, '%d/%m/%Y')
    return x

def normalize_data(code):
  df = pd.read_json('data/data_olhc/{}.json'.format(code), convert_dates=False)
  df = df.drop_duplicates().reset_index(drop=True)  
  df.date = df.date.apply(convertToDate)
  df = df.sort_values(by='date').reset_index(drop=True)
  for column in ['close', 'high', 'open', 'low']:
    df[column] = df[column].apply(toFloat)
  for column in ['value', 'volume']:
    df[column] = df[column].apply(toInt)
  return df

def computeEMA(data, com = 0.5):
    ema = data.ewm(com= com).mean()
    return ema

def computeRSI (data, time_window = 14):
    diff = data.diff(1).dropna()        # diff in one field(one day)

    #this preservers dimensions off diff values
    up_chg = 0 * diff
    down_chg = 0 * diff
    
    # up change is equal to the positive difference, otherwise equal to zero
    up_chg[diff > 0] = diff[ diff>0 ]
    
    # down change is equal to negative deifference, otherwise equal to zero
    down_chg[diff < 0] = diff[ diff < 0 ]
    
    # check pandas documentation for ewm
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.ewm.html
    # values are related to exponential decay
    # we set com=time_window-1 so we get decay alpha=1/time_window
    up_chg_avg   = up_chg.ewm(com=time_window-1 , min_periods=time_window).mean()
    down_chg_avg = down_chg.ewm(com=time_window-1 , min_periods=time_window).mean()
    
    rs = abs(up_chg_avg/down_chg_avg)
    rsi = 100 - 100/(1+rs)
    return rsi

SYMBOL_LIST = [
    "STB", "VIC", "SSI", "MSN", "FPT", "HAG", "KDC", "EIB", "DPM", "VNM",
    ]   

TIME_ZONE = 'US/Eastern'

def prev_weekday(adate):
    while adate.weekday() >= 5: # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate

