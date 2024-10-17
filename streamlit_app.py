import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Kết nối tới Cassandra
def create_cassandra_connection():
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect('spark_streams')  # Kết nối đến keyspace đã tạo
    return session

# Lấy dữ liệu từ Cassandra
def get_bitcoin_data(session):
    query = "SELECT timestamp, google_finance, coinmarketcap FROM spark_streams.bitcoin_prices"
    rows = session.execute(query)
    data = [(row.timestamp, row.google_finance, row.coinmarketcap) for row in rows]
    return pd.DataFrame(data, columns=['timestamp', 'google_finance', 'coinmarketcap'])

# Thiết lập giao diện Streamlit
st.title("So sánh Giá Bitcoin theo Thời gian Thực")

# Kết nối đến Cassandra
session = create_cassandra_connection()

# Lấy dữ liệu và hiển thị
if session is not None:
    data = get_bitcoin_data(session)

    if not data.empty:
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data['google_finance'] = data['google_finance'].astype(float)
        data['coinmarketcap'] = data['coinmarketcap'].astype(float)

        # Vẽ biểu đồ
        plt.figure(figsize=(12, 6))
        plt.plot(data['timestamp'], data['google_finance'], label='Giá Bitcoin từ Google Finance', color='blue')
        plt.plot(data['timestamp'], data['coinmarketcap'], label='Giá Bitcoin từ CoinMarketCap', color='orange')
        plt.xlabel('Thời gian')
        plt.ylabel('Giá Bitcoin (USD)')
        plt.title('So sánh Giá Bitcoin giữa Google Finance và CoinMarketCap')
        plt.legend()
        plt.grid()

        # Định dạng trục thời gian
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.gcf().autofmt_xdate()

        # Hiển thị biểu đồ trong Streamlit
        st.pyplot(plt)
    else:
        st.write("Không có dữ liệu nào để hiển thị.")
else:
    st.write("Không thể kết nối đến Cassandra.")

