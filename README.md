# 🌍 Tourism Dashboard – Data Engineering Project

A scalable end-to-end **data engineering pipeline** for tourism analytics using real-time streaming, big data processing, workflow orchestration, and interactive visualization.

---

## 🚀 Project Overview

The Tourism Dashboard system processes tourism-related data using a modern data engineering stack. It ingests streaming data, processes it at scale, stores structured data, and visualizes insights through an interactive dashboard.

The system helps analyze:
- Tourist inflow trends
- Popular destinations
- Seasonal demand patterns
- Data-driven tourism insights

---

## 🏗️ System Architecture

**Data Pipeline Flow:**

Kafka → Spark → PostgreSQL → Airflow → Streamlit Dashboard

---

## ⚙️ Tech Stack

- 🐍 Python – Core programming language  
- 🔥 Apache Kafka – Real-time data streaming & ingestion  
- ⚡ Apache Spark – Distributed data processing & analytics  
- 🔄 Apache Airflow – Workflow orchestration & scheduling  
- 🗄️ PostgreSQL – Data storage and management  
- 🔌 psycopg2 – Database connectivity (Python ↔ PostgreSQL)  
- 📊 Streamlit – Interactive dashboard UI  
- 🐼 Pandas – Data cleaning and transformation  
- 📈 Plotly / Matplotlib – Data visualization  
- 🧰 Git & GitHub – Version control  
- 💻 Linux – Development environment  

---

## 📊 Features

- Real-time data ingestion using Kafka  
- Large-scale data processing using Spark  
- Automated ETL pipelines using Airflow  
- Structured storage in PostgreSQL  
- Interactive dashboard built with Streamlit  
- Visual analytics for tourism trends  
- Modular and scalable architecture  

---

## ⭐ If you like this project

Give it a ⭐ on GitHub and feel free to contribute!

---

## 🐳 Docker

Run the full local stack with:

```bash
docker compose up --build
```

Services included:
- PostgreSQL
- Kafka and ZooKeeper
- Seed job for the analytics tables
- Streaming producer and consumer
- Spark streaming job
- Streamlit dashboard

The dashboard will be available at `http://localhost:8501`.

---

## 🚀 Deployment

### Deploy with Docker

This repository now includes a deployment-ready `Dockerfile` that starts the Streamlit dashboard automatically.

Build and run it locally:

```bash
docker build -t tourism-dashboard .
docker run -p 8501:8501 --env-file .env tourism-dashboard
```

If your host provides a `PORT` variable, the container will use it automatically.

### Deploy on Streamlit Community Cloud

Streamlit Community Cloud does not use the `Dockerfile`. Use the GitHub repo directly and point the app to:

```text
dashboard/live_dashboard.py
```

Then set `DATABASE_URL` in the Streamlit Cloud secrets UI.

If you prefer the simpler non-streaming dashboard, you can deploy:

```text
dashboard/app.py
```

Make sure the deployed app has access to the Railway PostgreSQL connection string.
