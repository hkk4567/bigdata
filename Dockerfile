# Môi trường tái lập cho đồ án Big Data (PySpark 3.5.3 + Pandas/PyArrow + Java 17).
# Build:  docker compose build        Chạy demo:  docker compose run --rm app
FROM python:3.11-slim-bookworm

# Spark cần Java; procps cho lệnh ps (Spark dùng); không cài gì khác.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Chỉ chép mã nguồn; dữ liệu (data/) và kết quả (results/) được gắn volume khi chạy (xem docker-compose.yml)
COPY jobs ./jobs
COPY sql ./sql
COPY scripts ./scripts
COPY tests ./tests
COPY docs ./docs
COPY README.md NOTES.md ./

# Mặc định: demo trọn pipeline trên dữ liệu GIẢ nhỏ (không cần tải gì, không đụng data/ và results/ của bạn)
CMD ["bash", "scripts/docker_demo.sh"]
