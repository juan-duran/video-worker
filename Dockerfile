FROM python:3.11-slim

# ffmpeg for muxing
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Render sets PORT; keep a default for local runs
ENV PORT=8080
EXPOSE 8080

# Use sh -c so ${PORT} expands
CMD ["sh","-c","uvicorn app:app --host 0.0.0.0 --port ${PORT}"]
