FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Безбуферный вывод, чтобы логи сразу шли в docker logs
ENV PYTHONUNBUFFERED=1

# Запуск именно client.py и ещё + -u для надёжности
CMD ["python", "-u", "client.py"]
