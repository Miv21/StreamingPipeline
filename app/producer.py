import os
import json
import time
import requests
from pathlib import Path
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')

#Настройки
KAFKA_TOPIC = 'Prices'
# Берем адрес сервера из env (по умолчанию адрес сервера localhost:9092)
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
#Выбираем популярные активы для демонстрации (Bitcoin, Ethereum, Solana, Binance Coin)
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']

#Инициализация продюсера
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def LivePrice(symbol):
    #Получение цены через публичный HTTPS запрос к Binance
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        response = requests.get(url, timeout=5)
        response.raise_for_status() #Проверка на ошибки (4xx, 5xx)
        
        data = response.json()
        return {
            'symbol': symbol.replace('USDT', ''),
            'price': float(data['price']),

            'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'source': 'Binance_Public_API'
        }
    except Exception as e:
        print(f"Ошибка при получении {symbol}: {e}")
        return None

print("Producer запущен. Собираю рыночные данные...")

try:
    while True:
        for symbol in SYMBOLS:
            payload = LivePrice(symbol)
            if payload:
                producer.send(KAFKA_TOPIC, payload)
                print(f"Отправлено: {payload['symbol']} - ${payload['price']}")
        
        # Делаем паузу, чтобы не спамить API и базу слишком часто
        time.sleep(2) 
except KeyboardInterrupt:
    print("Работа продюсера остановлена.")
finally:
    producer.close()