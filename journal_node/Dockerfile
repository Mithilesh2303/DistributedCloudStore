FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./journal_node

EXPOSE 8010

CMD ["uvicorn", "journal_node.src.main:app", "--host", "0.0.0.0", "--port", "8010"]

