FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data
ENV DATABASE_PATH=/data/memoria.db
ENV SCHEDULER_ENABLED=1

EXPOSE 8080
CMD ["python", "run.py"]
