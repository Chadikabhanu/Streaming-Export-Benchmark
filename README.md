\# Streaming Export Benchmark



A high-performance Node.js utility for benchmarking streaming database exports in various formats (CSV, JSON, XML, Parquet).



\## 🚀 Overview

This project demonstrates how to export large datasets (100k+ rows) from PostgreSQL using Node.js streams. It is designed to be memory-efficient, using a constant low memory footprint regardless of the dataset size.



\### Features

\- \*\*Streaming Architecture:\*\* Uses `pg-query-stream` and Node.js `Transform` streams to handle backpressure.

\- \*\*Multiple Formats:\*\* Supports CSV, JSON, XML, and Parquet.

\- \*\*Dockerized:\*\* seamless "one-command" setup using Docker Compose.

\- \*\*Performance Metrics:\*\* Measures duration, file size, and peak memory usage.



\## 🛠️ Setup \& Run



\### Prerequisites

\- Docker \& Docker Compose



\### 1. Start Environment

Run the database and application container:

```bash

docker-compose up -d --build

