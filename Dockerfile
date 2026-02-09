FROM python:3.11-slim

WORKDIR /app

# Upgrade pip and install requirements if provided
RUN pip install -U pip

# Copy requirements.txt if exists (compose mounts source later)
COPY requirements.txt /tmp/requirements.txt
RUN if [ -s /tmp/requirements.txt ]; then pip install -r /tmp/requirements.txt; fi

# Default command is empty; compose will override with volume-mounted workdir
CMD ["bash", "-lc", "echo dockerfile_built && sleep infinity"]
