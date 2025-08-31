# K-Scrape Nexus Development Environment

This file sets up a complete development environment for K-Scrape Nexus using Docker Compose.

## Services Included

### Core Infrastructure
- **PostgreSQL** (port 5432): Main database
- **Redis** (port 6379): Cache and message queue
- **MinIO** (ports 9000/9001): S3-compatible object storage

### Monitoring Stack
- **Prometheus** (port 9090): Metrics collection
- **Grafana** (port 3000): Dashboards and visualization
- **Jaeger** (port 16686): Distributed tracing

### Management Tools
- **pgAdmin** (port 8080): PostgreSQL administration
- **Redis Commander** (port 8081): Redis management

## Quick Start

1. **Start the environment**:
```bash
docker-compose up -d
```

2. **Check service health**:
```bash
docker-compose ps
```

3. **View logs**:
```bash
docker-compose logs -f [service-name]
```

## Access URLs

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jaeger**: http://localhost:16686
- **MinIO Console**: http://localhost:9001 (minio_admin/minio_password)
- **pgAdmin**: http://localhost:8080 (admin@k-scrape-nexus.com/admin)
- **Redis Commander**: http://localhost:8081

## Environment Variables

Create a `.env` file in the project root with:

```env
# Database
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/k_scrape_nexus

# Redis
REDIS_URL=redis://:redis_password@localhost:6379/0

# MinIO/S3
S3_BUCKET=k-scrape-nexus
S3_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minio_admin
AWS_SECRET_ACCESS_KEY=minio_password

# Application
SECRET_KEY=your-secret-key-here
DEBUG=true
LOG_LEVEL=DEBUG
```

## Data Persistence

All data is persisted using Docker volumes:
- `postgres_data`: PostgreSQL data
- `redis_data`: Redis data
- `minio_data`: MinIO/S3 storage
- `grafana_data`: Grafana dashboards and settings
- `prometheus_data`: Prometheus metrics

## Cleanup

To stop and remove all containers and volumes:

```bash
docker-compose down -v
```

## Health Checks

All services include health checks. You can monitor them with:

```bash
docker-compose ps
```

Services should show as "healthy" once fully started.

## Networking

All services are connected via the `k-scrape-network` bridge network, allowing internal communication while exposing necessary ports to the host.