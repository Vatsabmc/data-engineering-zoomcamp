# Week 1: Docker & Terraform

## Overview

This week focuses on containerizing a data ingestion pipeline and setting up infrastructure with Terraform. We build a Python application that ingest NYC taxi data into PostgreSQL, containerize it with Docker, and manage it with Docker Compose.

## Project Architecture

- **Data Source**: NYC TLC (Taxi & Limousine Commission) data in Parquet/CSV format
- **Database**: PostgreSQL (containerized)
- **Application**: Python-based data ingestion script
- **Orchestration**: Docker & Docker Compose
- **IaC**: Terraform (for cloud infrastructure)

## 📦 Prerequisites

- Docker & Docker Compose
- Python 3.13+
- PostgreSQL (via Docker)
- uv package manager

## 🏗️ Setup Instructions

### 1. Create Docker Network

```bash
docker network create pg-network
```

### 2. Start PostgreSQL Container

```bash
docker-compose -f 01-docker-terraform/docker-compose.yaml up -d
```

### 3. Build the Ingestion Image

From the **parent directory**:
```bash
docker build -t taxi_ingest:v001 -f 01-docker-terraform/Dockerfile .
```

## 🔄 Running the Data Pipeline

### Local Execution (Development)

Ingest 2021-01 data:
```bash
uv run python pipeline/ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips_2021_1 \
  --year=2021 \
  --month=1 \
  --chunksize=100000
```

### Docker Execution (CSV Format)

```bash
docker run -it --rm \
  --network=docker-compose-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --data-format=csv \
    --target-table=yellow_taxi_trips_2021_1 \
    --year=2021 \
    --month=1 \
    --chunksize=100000
```

### Docker Execution (Parquet Format)

```bash
docker run -it --rm \
  --network=docker-compose-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --data-format=parquet \
    --target-table=ny_taxi_trips \
    --year=2025 \
    --month=11 \
    --chunksize=100000
```

## 🛑 Cleanup

Stop and remove containers:
```bash
docker-compose -f 01-docker-terraform/docker-compose.yaml down
```

Remove Docker network:
```bash
docker network rm pg-network
```

## 📂 Project Structure

```
01-docker-terraform/
├── Dockerfile              # Container definition for ingestion app
├── docker-compose.yaml     # PostgreSQL and PgAdmin services
├── variables.tf            # Terraform variables
├── main.tf                 # Terraform configuration
├── terraform.tfstate       # Terraform state (git-ignored)
└── README.md               # This file
```

## 🔑 Key Learnings

### Docker Concepts

- Multi-stage builds
- Container networking
- Volume management
- Environment variables in containerized applications

### Data Ingestion Patterns

- Batch processing with chunking
- Streaming data to database
- Handling different data formats (CSV, Parquet)
- Error handling and retries

### Infrastructure as Code

*Terraform patterns and cloud deployment to be documented*

## 🐛 Troubleshooting

### Docker Network Issues
Ensure the `docker-compose-network` exists and containers are on the same network.

### PostgreSQL Connection Refused
- Check if PostgreSQL container is running: `docker ps`
- Verify hostname is correct (`pgdatabase` for Docker, `localhost` for local)
- Check port mapping in docker-compose.yaml

### File Not Found Errors
Build Docker image from the **parent directory**, not from `01-docker-terraform/`
