FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY templates/ ./templates/

RUN mkdir -p ./static

EXPOSE 6067

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "6067"]
