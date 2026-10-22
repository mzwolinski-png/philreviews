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

# --keep-alive 0: on a sync's graceful reload the old worker exits, and any idle
# connection Fly's proxy was holding to it came back as a 502.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "2", "--preload", "--timeout", "60", "--keep-alive", "0", "app:app"]
