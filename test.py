import requests
from bs4 import BeautifulSoup
from pipeline.util.util import convertTime, convertChange, convertPrice, convertToDate
from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Stock_platform')\
        .getOrCreate()

symbol = "FPT"
response = requests.get('https://s.cafef.vn/Lich-su-giao-dich-{}-1.chn#data'.format(symbol))
soup = BeautifulSoup(response.content, "html.parser")
info = soup.find("tr", id="ContentPlaceHolder1_ctl03_rptData2_altitemTR_1")
data = [{
        "symbol": symbol,
        "date": convertToDate(info.find("td", class_="Item_DateItem").text),
        "close": convertPrice(info.find_all("td", class_="Item_Price10")[1].text),
        "change": convertChange(info.find("td", class_="Item_ChangePrice").text),
        "volume": convertPrice(info.find_all("td", class_="Item_Price10")[2].text),
        "open": convertPrice(info.find_all("td", class_="Item_Price10")[5].text),
        "high": convertPrice(info.find_all("td", class_="Item_Price10")[6].text),
        "low": convertPrice(info.find_all("td", class_="Item_Price10")[7].text),
}]
print(spark.createDataFrame(data).head())
