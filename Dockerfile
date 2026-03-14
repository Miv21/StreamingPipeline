FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем всё содержимое проекта
COPY . .

# Чтобы Python видел модули внутри папки app
ENV PYTHONPATH=/app

# Контейнеры будут запускаться через команды в docker-compose