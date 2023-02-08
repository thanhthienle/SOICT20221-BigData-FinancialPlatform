from flask import Flask
from flask_cors import CORS, cross_origin
import pandas as pd
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from util.config import config
# on the terminal type: curl http://127.0.0.1:5000/



# auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
# cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT,auth_provider=auth_provider)
cluster = Cluster()

session = cluster.connect()
session.row_factory = dict_factory

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

#Tinh chi so RSI, truyen vao cung du lieu khi goi cac API cu co them 1 cot RSI
# Vẽ biểu đồ chứng khoán của mã code trong thời gian n ngày
@app.route('/OLHC/<code>/<n>', methods = ['GET'])
def getN(code, n):

    init_time = datetime.datetime.now() - datetime.timedelta( days = n)
    sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = {} and TIME>={};".format(CASSANDRA_DB, code, "HISTORICAL", init_time)
    df = pd.DataFrame()
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))
    df = df.reset_index(drop=True).fillna(pd.np.nan)
    return df

#Lấy giá cổ phiếu vào thời điểm đấy (vì mỗi 1 phút nó lại lưu thông tin vào intraday=> lấy thông tin gần nhất/ 5 dòng gần đây)
@app.route('/realtime/<code>/<minutes>', methods = ['GET'])
def getBang_rightnow(code, minutes):

    init_time = int(datetime.datetime.now().timestamp()*1000) - int(minutes)*60*1000
    sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = '{}' ORDER BY time DESC LIMIT 1;".format("stocks", "tick", code, init_time)
    df = pd.DataFrame()
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))
    df = df.reset_index(drop=True).fillna(pd.np.nan)
    
    return df.to_json()

@app.route('/news', methods = ['GET'])
def getNews():

    sql_query = "SELECT * FROM {}.{} LIMIT 6".format("stocks", "news")
    df = pd.DataFrame()
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))
    print(df.head())
    df = df.reset_index(drop=True).fillna(pd.np.nan)
    
    return df.to_json()

@app.route('/info/<code>', methods = ['GET'])
def getInfo(code):
    sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = '{}' ORDER BY time DESC LIMIT 2000".format("stocks", "historical", code)
    df = pd.DataFrame()
    print(df)
    # df = df.sort_values(by='time').reset_index(drop=True)
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))
    print(df.head())
    df = df.reset_index(drop=True).fillna(pd.np.nan)

    return df.to_json()

if __name__ == '__main__':
    app.run(debug = True)