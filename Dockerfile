FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY coordinator.py .
COPY worker.py .

RUN mkdir -p /app/txt /app/input_splits

CMD ["python", "worker.py"]

