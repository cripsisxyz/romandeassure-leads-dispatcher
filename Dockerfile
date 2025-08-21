FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Code
COPY . .

# Expose API
EXPOSE 8002

# Dossiers pour volumes
RUN mkdir -p /data /config

# CMD
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
