# syntax=docker/dockerfile:1

# Official Playwright base image: ships Chromium + all OS libraries needed for
# headless rendering, plus Python. Its tag must match the pinned playwright
# version in requirements.txt (1.49.0). Browsers live in /ms-playwright.
FROM mcr.microsoft.com/playwright/python:v1.49.0-noble

# Prevent Python from writing .pyc files and force unbuffered stdout/stderr so
# logs appear immediately in `docker logs`.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Rome

WORKDIR /app

# Install dependencies first to leverage Docker layer caching: this layer is
# rebuilt only when requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code.
COPY main.py .

# Run as a non-root user for safety. The Playwright base image already ships a
# non-root "pwuser" (UID 1000); reuse it rather than creating a colliding one.
# The pre-installed browsers under /ms-playwright are world-readable.
RUN mkdir -p /app/output \
    && chown -R pwuser:pwuser /app
USER pwuser

CMD ["python", "main.py"]
