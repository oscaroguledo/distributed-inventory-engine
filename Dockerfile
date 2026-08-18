FROM python:3.10-slim

RUN useradd --create-home --uid 10001 appuser
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY order_api ./order_api

ENV PYTHONUNBUFFERED=1

USER appuser
EXPOSE 8000
CMD ["uvicorn", "order_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
