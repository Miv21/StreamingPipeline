import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

#Загружаем переменные из .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')

#Настройки
KAFKA_TOPIC = 'Prices'
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
#Выбираем популярные активы для демонстрации (Bitcoin, Ethereum, Solana, Binance Coin)
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']

# Инициализация продюсера
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_market_data(symbols):
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        symbols_str = json.dumps(symbols, separators=(',', ':'))
        params = {'symbols': symbols_str}
        
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Ошибка при получении: {e}")
        return None

print("Producer запущен. Собираю рыночные данные...")

try:
    while True:
        raw_data = get_market_data(SYMBOLS)
        
        if raw_data:
            df = pd.DataFrame(raw_data)
            #Выбираем нужные поля и приводим к числам
            df = df[['symbol', 'lastPrice', 'volume', 'priceChangePercent', 'priceChange']]
            df['lastPrice'] = pd.to_numeric(df['lastPrice'])
            df['volume'] = pd.to_numeric(df['volume'])
            df['priceChangePercent'] = pd.to_numeric(df['priceChangePercent'])
            df['priceChange'] = pd.to_numeric(df['priceChange'])
            #Очистка имен и добавление метаданных
            df['symbol'] = df['symbol'].str.replace('USDT', '')
            df['timestamp'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            df['source'] = 'Binance_24h_Stats'
            #Конвертируем обратно в словари для Kafka
            payloads = df.to_dict(orient='records')
            
            for payload in payloads:
                producer.send(KAFKA_TOPIC, payload)
                print(f"Отправлено: {payload['symbol']} | Цена: {payload['lastPrice']} | Объем: {payload['volume']:.2f}")
        
        time.sleep(2) 
except KeyboardInterrupt:
    print("Остановка продюсера...")
finally:
    producer.close()