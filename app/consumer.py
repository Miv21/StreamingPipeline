import os
import json
import pandas as pd
from pathlib import Path
from kafka import KafkaConsumer
import clickhouse_connect
from dotenv import load_dotenv

#Загружаем переменные из .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
CH_HOST = os.getenv('CH_HOST')
CH_PORT = int(os.getenv('CH_PORT', 8443))
CH_USER = os.getenv('CH_USER', 'default')
CH_PASSWORD = os.getenv('CH_PASSWORD')

#Подключение к ClickHouse
try:
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, secure=True, verify=False,
        interface='https', username=CH_USER, password=CH_PASSWORD, database='default'
    )
    print("Успешное подключение к ClickHouse")
except Exception as e:
    print(f"Ошибка подключения: {e}"); exit()

#Создание таблиц
client.command('''
CREATE TABLE IF NOT EXISTS Prices (
    symbol String,
    price Float64,
    priceChange Float64,
    volume Float64,
    priceChangePercent Float64,
    timestamp String,
    source String DEFAULT 'Crypto'
) ENGINE = MergeTree() ORDER BY (timestamp, symbol)
''')

client.command('''
CREATE TABLE IF NOT EXISTS Stocks_Price (
    symbol String,
    price Float64,
    timestamp String,
    source String DEFAULT 'Finnhub'
) ENGINE = MergeTree() ORDER BY (timestamp, symbol)
''')

assets = {
    #Криптовалюты (Данные из таблицы Prices)
    'btc_Prices': 'BTC', 
    'eth_Prices': 'ETH', 
    'sol_Prices': 'SOL', 
    'bnb_Prices': 'BNB',
    
    #Акции (Данные из таблицы Stocks_Price)
    'googl_Prices': 'GOOGL', 
    'nvda_Prices': 'NVDA', 
    'msft_Prices': 'MSFT', 
    'qcom_Prices': 'QCOM',
    'apple_Prices': 'AAPL'
}

#Список тикеров, которые относятся к акциям (для логики выбора таблицы)
stock_tickers = ['GOOGL', 'NVDA', 'MSFT', 'QCOM', 'AAPL']

for view_name, symbol in assets.items():
    source_table = 'Stocks_Price' if symbol in stock_tickers else 'Prices'
    client.command(f"DROP VIEW IF EXISTS {view_name}")
    #Создаем заново
    client.command(f"""
        CREATE VIEW {view_name} AS 
        SELECT * FROM {source_table} 
        WHERE symbol = '{symbol}'
    """)

print(f"Все Views ({len(assets)}) успешно обновлены и синхронизированы.")

#Настройка Kafka Consumer
consumer = KafkaConsumer(
    'Prices', 'Stocks_Price',
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='latest',
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#Настройки батчинга
batch_limit = 4 
crypto_buffer = []

print(f"Consumer запущен. Лимит пачки: {batch_limit}. Слушаю Kafka...")



try:
    for message in consumer:
        data = message.value
        topic = message.topic

        if topic == 'Prices':
            #Обработка Крипты
            if 'lastPrice' in data:
                data['price'] = data.pop('lastPrice')
            
            crypto_buffer.append(data)

            if len(crypto_buffer) >= batch_limit:
                df = pd.DataFrame(crypto_buffer)
                client.insert_df('Prices', df)
                print(f"[Prices] Пакет из {len(df)} сохранен.")
                crypto_buffer.clear()

        elif topic == 'Stocks_Price':
            # Обработка Акций (Single Insert)
            row = [data.get('symbol'), data.get('price'), data.get('timestamp'), data.get('source', 'Finnhub')]
            client.insert('Stocks_Price', [row], column_names=['symbol', 'price', 'timestamp', 'source'])
            print(f"[Stocks] Сохранено: {data['symbol']}")

except KeyboardInterrupt:
    print("Остановка консьюмера...")
finally:
    consumer.close()