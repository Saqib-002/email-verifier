FROM python:3.11-slim

# Install gcloud SDK + dependencies
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    gnupg \
    curl \
    && curl https://sdk.cloud.google.com | bash \
    && exec -l $SHELL \
    && ln -s /root/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify gcloud
RUN gcloud version

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]