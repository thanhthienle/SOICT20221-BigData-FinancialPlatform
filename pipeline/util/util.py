from datetime import timedelta
import time
import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, udf, col
from pyspark.sql.types import StringType, FloatType

def string_to_float(string):
    if isinstance(string, str):
        return float(string)
    else:
        return string

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
    return float(x.split(" %")[0])/100

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
      ts = time.mktime(datetime.datetime.strptime(x, "%d/%m/%Y").timetuple()) + 3600*7
      return datetime.datetime.fromtimestamp(ts)
    return x

def normalize_data(code):
  spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Stock_platform')\
        .getOrCreate()
  
  df = spark.read.option("multiline","true").json('data/data_olhc/{}.json'.format(code))
  df = df.dropDuplicates()
  df = df.withColumn("date", to_date(df.date, "dd/MM/yyyy"))  
  df = df.sort(df.date.asc())
  udfToFloat = udf(toFloat, FloatType())
  udfToInt = udf(toInt, StringType()) 
  for column in ['close', 'high', 'open', 'low']:
    df = df.withColumn(column, udfToFloat(col(column)))
  for column in ['value', 'volume']:
    df = df.withColumn(column, udfToInt(col(column)))
  return df

def calculateEma(data, n_days=25):
    window = Window.partitionBy('symbol').orderBy("date").rowsBetween(0, n_days - 1)
    alpha = 2 / (n_days + 1)
    ema = data.withColumn("EMA", F.avg(data["close"]).over(window))
    for i in range(1, n_days):
        ema = ema.withColumn("EMA", (data["close"] * alpha) + (ema["ema"] * (1 - alpha)))
    return ema

def calculateRsi(data, n_days=25):
    # Calculate the difference between consecutive "close" values
    data = data.withColumn("diff", data["close"].cast("double") - F.lag(data["close"]).over(Window.partitionBy('symbol').orderBy("date")))
    
    # Calculate the gain and loss over the specified number of days
    gain = data.withColumn("Gain", F.when(data["diff"] > 0, data["diff"]).otherwise(0.0))
    loss = data.withColumn("Loss", F.when(data["diff"] < 0, -data["diff"]).otherwise(0.0))
    
    # Calculate the average gain and average loss over the specified number of days
    avg_gain = gain.withColumn("avg_gain", F.avg("Gain").over(Window.partitionBy('symbol').orderBy("date").rowsBetween(-n_days, 0)))
    avg_loss = loss.withColumn("avg_loss", F.avg("Loss").over(Window.partitionBy('symbol').orderBy("date").rowsBetween(-n_days, 0)))
    avg_loss = avg_loss.select("date", "avg_loss")  
    # Combine the gain, loss, average gain and average loss into a single DataFrame
    combined = avg_gain.join(avg_loss, on=["date"], how="outer")
    
    # Calculate the relative strength
    relative_strength = combined.withColumn("relative_strength", combined["avg_gain"] / combined["avg_loss"])
    
    # Calculate the RSI
    rsi = relative_strength.withColumn("RSI", 100 - (100 / (1 + relative_strength["relative_strength"])))
    cols = ("diff","Gain","Loss", "avg_gain", "avg_loss", "relative_strength")
    return rsi.drop(*cols)

def computeEMA(data, com = 0.5):
    ema = data.ewm(com= com).mean()
    return ema

def computeRSI (data, time_window = 14):
    diff = data.diff(1).dropna()        # diff in one field(one day)

    #this preservers dimensions off diff values
    up_chg = 0 * diff
    down_chg = 0 * diff

SYMBOL_LIST = [
    "STB", "VIC", "SSI", "MSN", "FPT", "HAG", "KDC", "EIB", "DPM", "VNM",
    ]   

def prev_weekday(adate):
    while adate.weekday() >= 5: # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate

