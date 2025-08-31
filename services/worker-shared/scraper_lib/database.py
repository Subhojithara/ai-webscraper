"""
Database models and session management using SQLAlchemy with async support.
"""

from typing import AsyncGenerator, Optional, List
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4
import uuid

from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func

from .config import get_settings

# Base class for all models
Base = declarative_base()

# Enums for status tracking
class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class WorkerType(str, Enum):
    HTTP = "http"
    HEADLESS = "headless"
    PROCESSING = "processing"
    CRAWL4AI = "crawl4ai"


# Database Models
class Job(Base):
    """Main job entity representing a scraping job."""
    __tablename__ = "jobs"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[JobStatus] = mapped_column(String(20), nullable=False, default=JobStatus.PENDING)
    
    # File information
    file_path: Mapped[str] = mapped_column(String(500), nullable=False)
    file_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    file_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Progress tracking
    total_urls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_urls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_urls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Configuration and metadata
    config: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    job_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # Relationships
    tasks: Mapped[List["Task"]] = relationship("Task", back_populates="job", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return f"<Job(id={self.id}, name='{self.name}', status='{self.status}')>"
    
    @property
    def progress_percentage(self) -> float:
        """Calculate job completion percentage."""
        if self.total_urls == 0:
            return 0.0
        return (self.completed_urls / self.total_urls) * 100.0
    
    @property
    def is_complete(self) -> bool:
        """Check if job is complete."""
        return self.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]


class Task(Base):
    """Individual scraping task for a URL."""
    __tablename__ = "tasks"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    
    # Task details
    url: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[TaskStatus] = mapped_column(String(20), nullable=False, default=TaskStatus.PENDING)
    worker_type: Mapped[WorkerType] = mapped_column(String(20), nullable=False, default=WorkerType.HTTP)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    
    # Retry logic
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    
    # Results
    result_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    content_length: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Error handling
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Performance metrics
    duration_seconds: Mapped[Optional[float]] = mapped_column(nullable=True)
    
    # Configuration
    config: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    task_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # Relationships
    job: Mapped[Job] = relationship("Job", back_populates="tasks")
    result: Mapped[Optional["Result"]] = relationship("Result", back_populates="task", uselist=False)
    
    def __repr__(self) -> str:
        return f"<Task(id={self.id}, url='{self.url[:50]}...', status='{self.status}')>"
    
    @property
    def can_retry(self) -> bool:
        """Check if task can be retried."""
        return self.retry_count < self.max_retries and self.status == TaskStatus.FAILED


class Result(Base):
    """Scraped data result for a task."""
    __tablename__ = "results"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tasks.id"), nullable=False)
    
    # Scraped content
    url: Mapped[str] = mapped_column(String(2000), nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Extracted data
    links: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    images: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    emails: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    phone_numbers: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    
    # Metadata
    language: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    keywords: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Technical details
    response_headers: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    cookies: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    javascript_errors: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    
    # Quality metrics
    content_quality_score: Mapped[Optional[float]] = mapped_column(nullable=True)
    data_completeness: Mapped[Optional[float]] = mapped_column(nullable=True)
    
    # Timestamps
    extracted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Additional metadata
    result_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # Relationships
    task: Mapped[Task] = relationship("Task", back_populates="result")
    
    def __repr__(self) -> str:
        return f"<Result(id={self.id}, url='{self.url[:50]}...', title='{self.title[:30] if self.title else None}...')>"


# Database Engine and Session Management
class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(self, database_url: str, echo: bool = False):
        self.engine = create_async_engine(
            database_url,
            echo=echo,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session."""
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    async def create_tables(self):
        """Create all database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def drop_tables(self):
        """Drop all database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    
    async def close(self):
        """Close database connections."""
        await self.engine.dispose()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get global database manager instance."""
    global _db_manager
    if _db_manager is None:
        settings = get_settings()
        _db_manager = DatabaseManager(
            database_url=settings.database_url,
            echo=settings.debug
        )
    return _db_manager


async def get_database_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session dependency for FastAPI."""
    db_manager = get_database_manager()
    async for session in db_manager.get_session():
        yield session


# Repository Pattern Base Classes
class BaseRepository:
    """Base repository with common CRUD operations."""
    
    def __init__(self, session: AsyncSession, model_class):
        self.session = session
        self.model_class = model_class
    
    async def create(self, **kwargs):
        """Create a new record."""
        instance = self.model_class(**kwargs)
        self.session.add(instance)
        await self.session.flush()
        await self.session.refresh(instance)
        return instance
    
    async def get_by_id(self, id: uuid.UUID):
        """Get record by ID."""
        result = await self.session.get(self.model_class, id)
        return result
    
    async def update(self, id: uuid.UUID, **kwargs):
        """Update record by ID."""
        instance = await self.get_by_id(id)
        if instance:
            for key, value in kwargs.items():
                setattr(instance, key, value)
            await self.session.flush()
            await self.session.refresh(instance)
        return instance
    
    async def delete(self, id: uuid.UUID):
        """Delete record by ID."""
        instance = await self.get_by_id(id)
        if instance:
            await self.session.delete(instance)
            await self.session.flush()
        return instance is not None