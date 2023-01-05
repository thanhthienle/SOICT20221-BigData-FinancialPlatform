from flask import Flask
import pandas as pd
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# on the terminal type: curl http://127.0.0.1:5000/



# auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
# cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT,auth_provider=auth_provider)
cluster = Cluster()

session = cluster.connect()
session.row_factory = dict_factory

app = Flask(__name__)
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
    sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = '{}' and TIME>={};".format("stocks", "tick", code, init_time)
    df = pd.DataFrame()
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))

    df = df.reset_index(drop=True).fillna(pd.np.nan)

    return df.to_json()

if __name__ == '__main__':
    app.run(debug = True)