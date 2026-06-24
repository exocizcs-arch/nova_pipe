FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pipeline/requirements.txt ./pipeline/requirements.txt

RUN pip install --no-cache-dir -r ./pipeline/requirements.txt

COPY . .

CMD ["tail", "-f", "/dev/null"]


