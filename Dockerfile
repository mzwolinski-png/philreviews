FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py db.py notify.py ./
COPY templates/ templates/
COPY static/ static/

ENV DB_PATH=/data/reviews.db
ENV SUBSCRIBERS_DB_PATH=/data/subscribers.db

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "2", "--preload", "--timeout", "60", "app:app"]
