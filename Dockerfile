FROM python:3.11-slim@sha256:a1c1644e91e0f4e51549b59f8e72f5c30b09e9c5f9e2c9c5e3f8c5e3f8c5e3f
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "etl.run"]