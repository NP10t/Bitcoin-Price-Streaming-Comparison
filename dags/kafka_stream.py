import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import time
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import locale

default_args = {
    'owner': 'nguyenphuc',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

locale.setlocale(locale.LC_ALL, '')

def fetch_price_google_finance():
    url = "https://www.google.com/finance/quote/BTC-VND?sa=X&ved=2ahUKEwiO9oCEvIOJAxVflFYBHfW0O4YQ-fUHegQIHRAf"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
    }

    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')
    price_element = soup.find('div', class_='YMlKec fxKbKc')
    price = price_element.text
    return price


def fetch_bitcoin_price_binance():
    # Lấy giá Bitcoin từ Binance
    url = "https://api.binance.com/api/v3/ticker/price"
    params = {"symbol": "BTCUSDT"}
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    price_usd = float(data['price'])
    
    # Lấy tỷ giá USD sang VND
    exchange_rate_url = "https://api.exchangerate-api.com/v4/latest/USD"  # Hoặc một API khác
    exchange_rate_response = requests.get(exchange_rate_url)
    exchange_rate_response.raise_for_status()
    
    exchange_rate_data = exchange_rate_response.json()
    exchange_rate_vnd = exchange_rate_data['rates']['VND']
    
    # Tính toán giá Bitcoin bằng VND
    price_vnd = price_usd * exchange_rate_vnd
    
    # return f"{price_vnd:.2f} VND"
    formatted_price = locale.format_string('%.2f', price_vnd, grouping=True)
        
    return formatted_price

def fetch_bitcoin_prices():
    price_google_finance = fetch_price_google_finance()
    price_coinmarketcap = fetch_bitcoin_price_binance()
    return price_google_finance, price_coinmarketcap


# print(fetch_bitcoin_prices())

# Hàm để stream giá Bitcoin vào Kafka
def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 120:  # 2 phút
            break
        
        try:
            # Lấy giá Bitcoin
            price_google_finance, price_coinmarketcap = fetch_bitcoin_prices()
            if 1:
                data = {
                'google_finance': price_google_finance,
                'coinmarketcap': price_coinmarketcap,
                'timestamp': str(datetime.now())}
                producer.send('bitcoin_prices', json.dumps(data).encode('utf-8'))
                print(f"Sent data to Kafka: {data}")
            else:
                logging.error("No price data available.")
                
            time.sleep(3)  # Đợi 3 giây trước khi lấy lại giá Bitcoin

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging
    
#     res = get_data()
#     res = format_data(res)

#     producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
#     # curr_time = time.time()
#     producer.send('users_created', json.dumps(res).encode('utf-8'))
    
# stream_data()

with DAG('bitcoin_price_fetcher',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='fetch_price_task',
        python_callable=stream_data
    )
