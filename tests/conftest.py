"""
Pytest configuration and fixtures for K-Scrape Nexus tests.
"""

import asyncio
import os
import pytest
import pytest_asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from fastapi.testclient import TestClient
import redis.asyncio as redis

# Set up test environment variables BEFORE importing scraper_lib
os.environ.update({
    'DATABASE_URL': 'sqlite+aiosqlite:///:memory:',
    'REDIS_URL': 'redis://localhost:6379/1', 
    'S3_BUCKET': 'test-bucket',
    'S3_ENDPOINT_URL': 'http://localhost:9000',
    'AWS_ACCESS_KEY_ID': 'testkey',
    'AWS_SECRET_ACCESS_KEY': 'testsecret',
    'SECRET_KEY': 'test-secret-key-for-testing-only',
    'DEBUG': 'true',
    'LOG_LEVEL': 'DEBUG',
    'METRICS_ENABLED': 'false',
    'TRACING_ENABLED': 'false'
})

# Add paths for imports
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "services", "worker-shared"))
sys.path.append(os.path.join(os.path.dirname(__file__), "services", "api-server"))

from scraper_lib.database import Base
from scraper_lib.config import get_settings
from scraper_lib import get_redis_client


# Event loop fixture for async tests
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Database fixtures
@pytest_asyncio.fixture(scope="session")
async def test_db_engine():
    """Create test database engine."""
    settings = get_settings()
    engine = create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True
    )
    
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    # Cleanup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()


@pytest_asyncio.fixture
async def test_db_session(test_db_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create test database session."""
    async_session = async_sessionmaker(
        test_db_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# Redis fixtures
@pytest_asyncio.fixture
async def test_redis_client():
    """Create test Redis client."""
    settings = get_settings()
    client = redis.from_url(settings.redis_url)
    
    # Clear test database
    await client.flushdb()
    
    yield client
    
    # Cleanup
    await client.flushdb()
    await client.close()


# Mock fixtures
@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    mock_client = MagicMock()
    
    # Mock common S3 operations
    mock_client.get_object.return_value = {
        'Body': MagicMock(read=MagicMock(return_value=b'test content'))
    }
    mock_client.put_object.return_value = {'ETag': '"test-etag"'}
    mock_client.head_object.return_value = {
        'ContentLength': 100,
        'ContentType': 'text/html',
        'LastModified': '2023-01-01T00:00:00Z'
    }
    mock_client.generate_presigned_url.return_value = "https://test-bucket.s3.amazonaws.com/test-key"
    
    return mock_client


@pytest.fixture
def mock_httpx_client():
    """Mock httpx client for HTTP requests."""
    mock_client = AsyncMock()
    
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html><head><title>Test Page</title></head><body>Test content</body></html>"
    mock_response.content = mock_response.text.encode()
    mock_response.headers = {"content-type": "text/html"}
    mock_response.url = "https://example.com"
    mock_response.history = []
    
    mock_client.get.return_value = mock_response
    mock_client.aclose = AsyncMock()
    
    return mock_client


# Application fixtures
@pytest.fixture
def test_client():
    """Create test client for API server."""
    from services.api_server.app.main import create_app
    
    app = create_app()
    return TestClient(app)


# Data fixtures
@pytest.fixture
def sample_job_data():
    """Sample job data for testing."""
    return {
        "name": "Test Job",
        "description": "Test job description",
        "file_path": "uploads/test/test-file.txt",
        "config": {
            "timeout": 30,
            "retries": 3
        }
    }


@pytest.fixture
def sample_urls():
    """Sample URLs for testing."""
    return [
        "https://example.com",
        "https://httpbin.org/get",
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://www.google.com",
        "https://github.com"
    ]


@pytest.fixture
def sample_html_content():
    """Sample HTML content for testing."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Page</title>
        <meta name="description" content="Test page description">
        <meta name="keywords" content="test, page, example">
    </head>
    <body>
        <h1>Test Heading</h1>
        <p>This is a test paragraph with <a href="https://example.com">a link</a>.</p>
        <img src="https://example.com/image.jpg" alt="Test image">
        <div>
            <h2>Another Heading</h2>
            <p>More content here.</p>
            <ul>
                <li>Item 1</li>
                <li>Item 2</li>
            </ul>
        </div>
        <p>Contact us at test@example.com or call (555) 123-4567</p>
    </body>
    </html>
    """


@pytest.fixture
def sample_csv_content():
    """Sample CSV content for testing."""
    return """url,title,description
https://example.com,Example Site,Example description
https://test.com,Test Site,Test description
https://demo.com,Demo Site,Demo description"""


@pytest.fixture
def sample_json_content():
    """Sample JSON content for testing."""
    return {
        "urls": [
            "https://api.example.com/endpoint1",
            "https://api.example.com/endpoint2",
            "https://api.example.com/endpoint3"
        ],
        "metadata": {
            "source": "test",
            "created_at": "2023-01-01T00:00:00Z"
        }
    }


# Performance testing fixtures
@pytest.fixture
def large_url_list():
    """Large list of URLs for performance testing."""
    return [f"https://example.com/page-{i}" for i in range(1000)]


# Environment fixtures
@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Setup test environment variables."""
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/1")
    monkeypatch.setenv("DEBUG", "true")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("SECRET_KEY", "test-secret-key")


# Cleanup fixtures
@pytest.fixture(autouse=True)
async def cleanup_after_test():
    """Cleanup after each test."""
    yield
    # Add any necessary cleanup here


# Helper functions
def create_mock_task(task_id=None, url="https://example.com", status="pending"):
    """Create a mock task for testing."""
    from scraper_lib.database import Task, TaskStatus, WorkerType
    import uuid
    
    return Task(
        id=task_id or uuid.uuid4(),
        job_id=uuid.uuid4(),
        url=url,
        status=TaskStatus(status),
        worker_type=WorkerType.HTTP,
        priority=1,
        retry_count=0,
        max_retries=3
    )


def create_mock_job(job_id=None, name="Test Job", status="pending"):
    """Create a mock job for testing."""
    from scraper_lib.database import Job, JobStatus
    import uuid
    
    return Job(
        id=job_id or uuid.uuid4(),
        name=name,
        status=JobStatus(status),
        file_path="test/file.txt",
        total_urls=10,
        completed_urls=0,
        failed_urls=0
    )