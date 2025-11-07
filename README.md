# Distributed MapReduce System with Docker

A distributed MapReduce implementation for word counting using Docker containers, RPyC for inter-process communication, and Python.

## Prerequisites

- Docker (version 20.10 or higher)
- Docker Compose (version 1.29 or higher)

## Quick Start

### 1. Clone/Download the Repository

```bash
git clone <your-repo-url>
cd map_reduce
```

### 2. Build and Run with Default Settings

```bash
# Run with 4 workers (default)
docker-compose up --build --scale worker=4
```

### 3. Run with enwik9 Dataset

```bash
DATASET_URL=http://mattmahoney.net/dc/enwik9.zip docker-compose up --build --scale worker=8
```

### 4. Dynamic Worker Scaling

**Scale to any number of workers**

```bash
# Run with 16 workers
docker-compose up --build --scale worker=16
```

### 5. Adjust Map/Reduce Task Counts

Adjust task granularity via environment variables (should be ≥ number of workers):

```bash
N_MAP=16 N_REDUCE=8 docker-compose up --build --scale worker=8
```

### 7. Stop and Clean Up

```bash
docker-compose down

docker-compose down -v
```

## Configuration Options

Environment variables can be set when running docker-compose:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATASET_URL` | `http://mattmahoney.net/dc/enwik9.zip` | URL to download dataset |
| `N_MAP` | `6` | Number of map tasks |
| `N_REDUCE` | `3` | Number of reduce regions |
