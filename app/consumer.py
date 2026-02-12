import os
import json
from pathlib import Path
from kafka import KafkaConsumer
import clickhouse_connect
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')
# Настройки из окружения
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
CH_HOST = os.getenv('CH_HOST')
CH_PORT = int(os.getenv('CH_PORT', 8443))
CH_USER = os.getenv('CH_USER')
CH_PASSWORD = os.getenv('CH_PASSWORD')
# 2. Подключаемся к ClickHouse
try:
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        secure=True,
        verify=False,
        interface='https',
        username=CH_USER,
        password=CH_PASSWORD,
        database='default'
    )
    print("Успешное HTTPS подключение к ClickHouse")
except Exception as e:
    print(f"Ошибка подключения: {e}")
    exit()

# 3. Создаем таблицу для КРИПТЫ (Prices) если её ещё нет.
client.command('''
CREATE TABLE IF NOT EXISTS Prices (
    symbol String,
    price Float64,
    timestamp String,
    source String DEFAULT 'Crypto'
) ENGINE = MergeTree()
ORDER BY (timestamp, symbol)
''')
# 4. Создаем таблицу для АКЦИЙ (Stocks_Price) если её ещё нет.
client.command('''
CREATE TABLE IF NOT EXISTS Stocks_Price (
    symbol String,
    price Float64,
    timestamp String,
    source String DEFAULT 'Finnhub'
) ENGINE = MergeTree()
ORDER BY (timestamp, symbol)
''')

# Объединяем все активы, для которых нужны отдельные Views
assets = {
    # Крипта
    'btc_Prices': 'BTC',
    'eth_Prices': 'ETH',
    'sol_Prices': 'SOL',
    'bnb_Prices': 'BNB',
    # Акции (новые)
    'googl_Prices': 'GOOGL',
    'nvda_Prices': 'NVDA',
    'msft_Prices': 'MSFT',
    'qcom_Prices': 'QCOM'
}

for view_name, symbol in assets.items():
    # Определяем, из какой таблицы брать данные для этой View
    # Если тикер в списке акций, берем из Stocks_Price, иначе из Prices
    source_table = 'Stocks_Price' if symbol in ['GOOGL', 'NVDA', 'MSFT', 'QCOM'] else 'Prices'

    client.command(f'''
    CREATE VIEW IF NOT EXISTS {view_name} AS 
    SELECT * FROM {source_table} WHERE symbol = '{symbol}'
    ''')

# 5. Настраиваем Kafka Consumer на ДВА топика
consumer = KafkaConsumer(
    'Prices', 
    'Stocks_Price',
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='latest',
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Консьюмер запущен. Слушаю топики: Prices, Stocks_Price...")

# 6. Цикл обработки сообщений
try:
    for message in consumer:
        data = message.value
        topic = message.topic  # Получаем имя топика (Prices или Stocks_Price)
        # Подготовка данных
        # Используем .get(), чтобы скрипт не падал, если какого-то поля нет
        row = [
            data.get('symbol'),
            data.get('price'),
            data.get('timestamp'),
            data.get('source', 'Unknown')
        ]
        # Вставляем данные именно в ту таблицу, которая совпадает с именем топика
        client.insert(
            topic, 
            [row], 
            column_names=['symbol', 'price', 'timestamp', 'source']
        )
        print(f"[{topic}] Сохранено: {data['symbol']} | Цена: {data['price']}")
except KeyboardInterrupt:
    print("Остановка консьюмера...")
finally:
    consumer.close()