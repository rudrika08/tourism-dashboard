FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    STREAMLIT_APP=dashboard/live_dashboard.py \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["sh", "-c", "export STREAMLIT_SERVER_PORT=${PORT:-$STREAMLIT_SERVER_PORT}; streamlit run ${STREAMLIT_APP} --server.headless true --server.address ${STREAMLIT_SERVER_ADDRESS} --server.port ${STREAMLIT_SERVER_PORT}"]