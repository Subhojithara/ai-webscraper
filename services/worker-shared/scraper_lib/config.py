"""
Configuration management using Pydantic Settings.
Supports environment variables and .env files.
"""

from typing import Optional, List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from functools import lru_cache
import os


class Settings(BaseSettings):
    """Application settings with validation and environment variable support."""
    
    # Application
    app_name: str = Field(default="k-scrape-nexus", env="APP_NAME")
    app_version: str = Field(default="1.0.0", env="APP_VERSION")
    debug: bool = Field(default=False, env="DEBUG")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Database
    database_url: str = Field(..., env="DATABASE_URL")
    database_pool_size: int = Field(default=10, env="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(default=20, env="DATABASE_MAX_OVERFLOW")
    
    # Redis
    redis_url: str = Field(..., env="REDIS_URL") 
    redis_max_connections: int = Field(default=50, env="REDIS_MAX_CONNECTIONS")
    
    # S3 Storage
    s3_bucket: str = Field(..., env="S3_BUCKET")
    s3_region: str = Field(default="us-east-1", env="S3_REGION")
    s3_endpoint_url: Optional[str] = Field(default=None, env="S3_ENDPOINT_URL")
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    
    # Queue Names
    queue_new_files: str = Field(default="new_files", env="QUEUE_NEW_FILES")
    queue_scrape_http: str = Field(default="scrape_http", env="QUEUE_SCRAPE_HTTP")
    queue_scrape_headless: str = Field(default="scrape_headless", env="QUEUE_SCRAPE_HEADLESS")
    queue_scrape_crawl4ai: str = Field(default="scrape_crawl4ai", env="QUEUE_SCRAPE_CRAWL4AI")
    queue_process_data: str = Field(default="process_data", env="QUEUE_PROCESS_DATA")
    queue_failed_tasks: str = Field(default="failed_tasks", env="QUEUE_FAILED_TASKS")
    queue_process_data: str = Field(default="process_data", env="QUEUE_PROCESS_DATA")
    queue_failed_tasks: str = Field(default="failed_tasks", env="QUEUE_FAILED_TASKS")
    
    # Worker Settings
    worker_concurrency: int = Field(default=10, env="WORKER_CONCURRENCY")
    worker_timeout: int = Field(default=300, env="WORKER_TIMEOUT") 
    worker_retry_attempts: int = Field(default=3, env="WORKER_RETRY_ATTEMPTS")
    worker_retry_delay: int = Field(default=60, env="WORKER_RETRY_DELAY")
    
    # Scraping Settings
    scraping_timeout: int = Field(default=30, env="SCRAPING_TIMEOUT")
    scraping_max_redirects: int = Field(default=10, env="SCRAPING_MAX_REDIRECTS")
    scraping_user_agents: List[str] = Field(
        default=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ],
        env="SCRAPING_USER_AGENTS"
    )
    
    # Proxy Settings
    proxy_enabled: bool = Field(default=False, env="PROXY_ENABLED")
    proxy_urls: List[str] = Field(default=[], env="PROXY_URLS")
    proxy_rotation_enabled: bool = Field(default=True, env="PROXY_ROTATION_ENABLED")
    
    # Rate Limiting
    rate_limit_enabled: bool = Field(default=True, env="RATE_LIMIT_ENABLED")
    rate_limit_requests_per_second: float = Field(default=10.0, env="RATE_LIMIT_RPS")
    rate_limit_burst: int = Field(default=20, env="RATE_LIMIT_BURST")
    
    # Monitoring & Observability
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    metrics_port: int = Field(default=8080, env="METRICS_PORT")
    tracing_enabled: bool = Field(default=True, env="TRACING_ENABLED")
    tracing_endpoint: Optional[str] = Field(default=None, env="TRACING_ENDPOINT")
    
    # AI/LLM Settings for Crawl4AI
    openai_api_key: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    anthropic_api_key: Optional[str] = Field(default=None, env="ANTHROPIC_API_KEY")
    llm_provider: str = Field(default="openai", env="LLM_PROVIDER")
    llm_model: str = Field(default="gpt-3.5-turbo", env="LLM_MODEL")
    enable_ai_extraction: bool = Field(default=False, env="ENABLE_AI_EXTRACTION")
    s3_enabled: bool = Field(default=True, env="S3_ENABLED")
    
    # Security
    secret_key: str = Field(..., env="SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    jwt_expire_minutes: int = Field(default=30, env="JWT_EXPIRE_MINUTES")
    
    # File Processing
    max_file_size_mb: int = Field(default=100, env="MAX_FILE_SIZE_MB")
    supported_file_extensions: List[str] = Field(
        default=[".txt", ".csv", ".json", ".xlsx", ".xls"],
        env="SUPPORTED_FILE_EXTENSIONS"
    )
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()
    
    @field_validator("scraping_user_agents", mode="before")
    @classmethod
    def parse_user_agents(cls, v):
        if isinstance(v, str):
            return [ua.strip() for ua in v.split(",") if ua.strip()]
        return v
    
    @field_validator("proxy_urls", mode="before")
    @classmethod
    def parse_proxy_urls(cls, v):
        if isinstance(v, str):
            return [url.strip() for url in v.split(",") if url.strip()]
        return v
    
    @field_validator("supported_file_extensions", mode="before")
    @classmethod
    def parse_file_extensions(cls, v):
        if isinstance(v, str):
            return [ext.strip() for ext in v.split(",") if ext.strip()]
        return v
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False
    }


@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()


# Environment-specific configurations
def get_development_settings() -> Settings:
    """Get development environment settings."""
    os.environ.setdefault("DEBUG", "true")
    os.environ.setdefault("LOG_LEVEL", "DEBUG")
    return get_settings()


def get_production_settings() -> Settings:
    """Get production environment settings.""" 
    os.environ.setdefault("DEBUG", "false")
    os.environ.setdefault("LOG_LEVEL", "INFO")
    return get_settings()


def get_test_settings() -> Settings:
    """Get test environment settings."""
    os.environ.setdefault("DEBUG", "true")
    os.environ.setdefault("LOG_LEVEL", "DEBUG")
    os.environ.setdefault("DATABASE_URL", "sqlite:///./test.db")
    os.environ.setdefault("REDIS_URL", "redis://localhost:6379/1")
    return get_settings()