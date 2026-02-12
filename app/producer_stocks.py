import os
import time
import json
import requests
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaProducer
from datetime import datetime, timezone

# Загружаем переменные из .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')

# --- КОНФИГУРАЦИЯ ---
KAFKA_TOPIC = 'Stocks_Price' 
# Берем адрес сервера и токен из env (по умолчанию адрес сервера localhost:9092)
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
FINNHUB_TOKEN = os.getenv('FINNHUB_TOKEN')

# Список акций для демонстрации (Google, Nvidia, Microsoft, Qualcomm)  
STOCKS = ['GOOGL', 'NVDA', 'MSFT', 'QCOM']

# Инициализация продюсера
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_stock_payload(symbol):
    try:
        # Запрос к Finnhub (API котировок в реальном времени)
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_TOKEN}"
        res = requests.get(url, timeout=5)
        
        if res.status_code == 200:
            data = res.json()
            # 'c' - это текущая цена (Current price) в ответе Finnhub
            price = float(data.get('c', 0))
            
            if price == 0:
                return None
            return {
                'symbol': symbol,
                'price': price,
                'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'source': 'Finnhub_API'
            }
        elif res.status_code == 429:
            print(f"[{symbol}] Лимит! Ждем 10 сек...")
            time.sleep(10)
        return None
    except Exception as e:
        print(f"Ошибка при получении {symbol}: {e}")
        return None

print(f"Producer запущен. Собираю рыночные данные...")

try:
    while True:
        for symbol in STOCKS:
            payload = get_stock_payload(symbol)
            if payload:
                producer.send(KAFKA_TOPIC, payload)
                print(f"Отправлено: {payload['symbol']} - ${payload['price']} (UTC Time: {payload['timestamp']})")
            # Делаем паузу 1.5 секунды между акциями для избежания блокировки на Finnhub.
            # 4 акции * 1.5 сек = 6 секунд на полный круг. Итого 40 запросов в минуту (Finnhub разрешает 60)
            time.sleep(1.5)
except KeyboardInterrupt:
    print("Работа продюсера остановлена.")
finally:
    producer.close()