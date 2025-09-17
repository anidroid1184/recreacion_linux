FROM mcr.microsoft.com/playwright/python:v1.54.0-noble

USER pwuser
WORKDIR /app

# Copy ONLY the subproject into /app/recreacion_linux
COPY --chown=pwuser:pwuser . /app/recreacion_linux

# Optional Xvfb for headed diagnostics
USER root
RUN apt-get update && apt-get install -y --no-install-recommends xvfb \
    && rm -rf /var/lib/apt/lists/*
USER pwuser

# Install dependencies for the subproject
RUN pip install --no-cache-dir -r /app/recreacion_linux/requirements.txt
RUN pip install --no-cache-dir "playwright==1.54.*"

# Output/traces folders
RUN mkdir -p /app/recreacion_linux/out /app/recreacion_linux/logs /app/recreacion_linux/traces

ENV PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS=1 \
    TZ=America/Bogota

# We'll run with: python -m recreacion_linux.main ...
CMD ["bash","-lc","python -V && echo Ready"]
# Dockerfile (scoped to recreacion_linux/)
# Base: Official Playwright image with browsers and OS deps preinstalled
FROM mcr.microsoft.com/playwright/python:v1.54.0-noble

USER pwuser
WORKDIR /app

# Build context is the repo root (.. from recreacion_linux/docker-compose.yml). We copy everything.
COPY --chown=pwuser:pwuser . /app

# (Optional) Headed support inside container (Xvfb)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends xvfb \
    && rm -rf /var/lib/apt/lists/*
USER pwuser

# Install project dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Ensure Python package version aligns with base image runtime
RUN pip install --no-cache-dir "playwright==1.54.*"

# Pre-create output folders
RUN mkdir -p /app/recreacion_linux/out /app/recreacion_linux/logs /app/logs

# Sensible defaults; override via docker-compose env_file or environment section
ENV PYTHONPATH=/app \
    HEADLESS=true \
    DEBUG_SCRAPER=false \
    BLOCK_RESOURCES=true \
    PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS=1 \
    TZ=America/Bogota

CMD ["bash","-lc","python -V && echo Ready"]
