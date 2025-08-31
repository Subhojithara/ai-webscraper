# K-Scrape Nexus Worker Shared Library

This directory contains the shared library used across all K-Scrape Nexus microservices.

## Components

- **`config.py`**: Application configuration management with Pydantic
- **`database.py`**: SQLAlchemy models and database session management
- **`cache.py`**: Redis client and queue management utilities
- **`observability.py`**: Logging, metrics, and tracing instrumentation
- **`utils.py`**: Common utility functions and helpers
- **`alembic/`**: Database migration management

## Usage

Install the shared library in development mode:

```bash
pip install -e services/worker-shared/
```

Then import in your services:

```python
from scraper_lib import get_settings, get_database_session, get_redis_client
from scraper_lib.observability import setup_logging, setup_metrics
```

## Database Migrations

Run migrations using Alembic:

```bash
cd services/worker-shared/scraper_lib
alembic upgrade head
```

Create new migration:

```bash
alembic revision --autogenerate -m "Description of changes"
```