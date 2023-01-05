from flask import Flask
import pandas as pd
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mpl_finance import candlestick_ohlc
# on the terminal type: curl http://127.0.0.1:5000/


auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT,auth_provider=auth_provider)

session = cluster.connect(CASSANDRA_DB)
session.row_factory = dict_factory

app = Flask(__name__)
# Vẽ biểu đồ chứng khoán của mã code trong thời gian n ngày
@app.route('/OLHC/<code>/<n>', method = ['GET'])
def getN(code, n):
    
    hehe = datetime.datetime.now() - datetime.timedelta( days = n)
    sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = {} and TIME>={};".format(CASSANDRA_DB, code, "HISTORICAL")
    df = pd.DataFrame()
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))

    df = df.reset_index(drop=True).fillna(pd.np.nan)
    return df

# Định vẽ đồ thị luôn mà không vẽ được
    # df.columns = ['TIME', 'SYMBOL','OPEN','HIGH','LOW','CLOSE','ADJUSTED_CLOSE','VOLUME','dividend_amount','split_coefficient']
    # df['TIME'] = df['TIME'].map(mdates.date2num)
    # ax = plt.subplot()
    # ax.grid(True)
    # ax.set_axisbelow(True)
    # ax.set_facecolor('black')
    # ax.set_title('Mã Cổ Phiếu {}'.format(code), color = 'white' )
    # ax.figure.set_facecolor('#121212')
    # ax.tick_params(axis= 'x', colors = 'white' )
    # ax.tick_params(axis= 'y', colors = 'white' )
    # ax.xaxis_date()
    # candlestick_ohlc(ax, [df['OPEN'],df['HIGH'],df['LOW'],df['CLOSE']] ,width=1, colorup='#00ff00' )
    # plt.show()
    

#Lấy giá cổ phiếu vào thời điểm đấy (vì mỗi 1 phút nó lại lưu thông tin vào intraday=> lấy thông tin gần nhất/ 5 dòng gần đây)
@app.route('/realtime/<code>/<minutes>', method = ['GET'])
def getBang_rightnow(code, minutes):
    
    hehe = datetime.datetime.now() - datetime.timedelta(minutes=minutes)
    sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = {} and TIME>={};".format(CASSANDRA_DB, code, "INTRADAY")
    df = pd.DataFrame()
    for row in session.execute(sql_query):
        df = df.append(pd.DataFrame(row, index=[0]))

    df = df.reset_index(drop=True).fillna(pd.np.nan)

    return df

if __name__ == '__main__':
  
    app.run(debug = True)