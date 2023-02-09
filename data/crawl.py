from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import json

chrome_options = Options()
chrome_options.add_argument("--incognito")
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_experimental_option("detach", True)

driver = webdriver.Chrome(options=chrome_options, executable_path="chromedriver")

def format_num(num):
  num = str(num)
  if len(num) < 2:
    return "0" + num
  return num

stock = ["STB", "VIC", "SSI", "MSN", "FPT", "HAG", "KDC", "EIB", "DPM", "VNM"]
# for page in range (2, 134)
for st in stock:
  data = []
  driver.get(f"https://s.cafef.vn/Lich-su-giao-dich-{st}-1.chn")
  time.sleep(1)
  for page in range (2, 121):
    for i in range (0, 20):
      row_num = format_num(i)
      sign = ""
      if(i%2 == 0):
        sign += "itemTR"
      else:
        sign += "altitemTR"
      # els = driver.find_element('id', f'ctl00_ContentPlaceHolder1_ctl03_rptData2_ctl{row_num}')
      els = driver.find_element('id', f'ContentPlaceHolder1_ctl03_rptData2_{sign}_{i}')
      row_data = {}
      row_data["date"] = els.find_element("class name", "Item_DateItem").text
      price_index = els.find_elements("class name", "Item_Price10")
      row_data["open"] = price_index[5].text
      row_data["high"] = price_index[6].text
      row_data["low"] = price_index[7].text
      row_data["close"] = price_index[1].text
      row_data["volume"] = price_index[2].text
      row_data["value"] = price_index[3].text
      data.append(row_data)
    els = driver.find_element('xpath', f'//a[@title=" Next to Page {page}"]')
    els.click()
    time.sleep(2)
    # if (page - 1) % 30 == 0:
    #   print(len(data))
    #   data_string = json.dumps(data, sort_keys=True, indent=4) 
    #   myjsonfile = open(f"{st[0]}{page - 1}.json", "w")
    #   myjsonfile.write(data_string)
    #   myjsonfile.close()
  data_string = json.dumps(data, sort_keys=True, indent=4) 
  myjsonfile = open(f"{st}.json", "w")
  myjsonfile.write(data_string)
  myjsonfile.close()
  print(st)

  # print(len(data))
  # data_string = json.dumps(data, sort_keys=True, indent=4) 
  # myjsonfile = open(f"{stock[st][0]}.json", "w")
  # myjsonfile.write(data_string)
  # myjsonfile.close()

