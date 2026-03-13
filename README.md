# Data Engineering Zoomcamp - Personal Implementation

Personal implementation of the DataTalks.Club Data Engineering Zoomcamp:
https://github.com/DataTalksClub/data-engineering-zoomcamp

This repository tracks hands-on work across the full course path, including
Docker/Terraform, orchestration, warehousing, analytics engineering,
data platforms, batch processing, and streaming.

## Course Context

The official Zoomcamp (2026 track) is organized into modules plus workshops and
a final project. This repo follows the same module naming pattern at the root:

- 01-docker-terraform
- 02-workflow-orchestration
- 03-data-warehouse
- 04-analytics-engineering
- 05-data-platforms
- 06-batch
- 07-streaming

## Repository Layout

Top-level folders in this implementation:

- [01-docker-terraform](01-docker-terraform)
- [02-workflow-orchestration](02-workflow-orchestration)
- [03-data-warehouse](03-data-warehouse)
- [04-analytics-engineering](04-analytics-engineering)
- [05-data-platforms](05-data-platforms)
- [06-batch](06-batch)
- [07-streaming](07-streaming)

Supporting files and outputs:

- [pyproject.toml](pyproject.toml), [uv.lock](uv.lock)
- [taxi_pipeline.duckdb](taxi_pipeline.duckdb), [duckdb.db](duckdb.db)
- [test.ipynb](test.ipynb)
- [logs](logs), [tmp](tmp)

## Progress by Module

1. [Module 1: Containerization and Infrastructure as Code](01-docker-terraform)
Work includes Docker Compose, ingestion scripts, SQL checks, and Terraform setup.

2. [Module 2: Workflow Orchestration](02-workflow-orchestration)
Contains multiple orchestration flows and cloud-oriented workflow variants.

3. [Module 3: Data Warehousing](03-data-warehouse)
Includes loading scripts and SQL for warehouse-style analysis.

4. [Module 4: Analytics Engineering](04-analytics-engineering)
dbt project and models in
[04-analytics-engineering/taxi_rides_ny](04-analytics-engineering/taxi_rides_ny).

5. [Module 5: Data Platforms](05-data-platforms)
Bruin pipelines and assets in
[05-data-platforms/my-first-pipeline](05-data-platforms/my-first-pipeline) and
[05-data-platforms/zoomcamp](05-data-platforms/zoomcamp).

6. [Module 6: Batch Processing](06-batch)
Spark and batch homework scripts with local data folders.

7. [Module 7: Streaming](07-streaming)
Kafka/Flink streaming jobs, producers/consumers, and homework SQL.

## Quick Start

Prerequisites:

- Python 3.13+
- uv
- Docker and Docker Compose

Setup:

```bash
uv sync
```

Then run module-specific tasks from each folder README or scripts. Current
module READMEs available in:

- [01-docker-terraform/README.md](01-docker-terraform/README.md)
- [04-analytics-engineering/taxi_rides_ny/README.md](04-analytics-engineering/taxi_rides_ny/README.md)
- [05-data-platforms/my-first-pipeline/README.md](05-data-platforms/my-first-pipeline/README.md)
- [05-data-platforms/zoomcamp/README.md](05-data-platforms/zoomcamp/README.md)

## Useful References

- Official course repo:
	https://github.com/DataTalksClub/data-engineering-zoomcamp
- Course FAQ:
	https://datatalks.club/faq/data-engineering-zoomcamp.html
- DataTalks.Club Slack:
	https://datatalks.club/slack.html
- NYC TLC trip records:
	https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Notes

This is a learning repository. Some implementations intentionally diverge from
official homework solutions while preserving the same concepts and objectives.
