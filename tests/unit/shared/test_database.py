"""
Unit tests for shared library database models and operations.
"""

import pytest
import uuid
from datetime import datetime, timezone
from sqlalchemy import select

from scraper_lib.database import Job, Task, Result, JobStatus, TaskStatus, WorkerType


class TestJobModel:
    """Test Job database model."""
    
    async def test_create_job(self, test_db_session):
        """Test creating a new job."""
        job = Job(
            name="Test Job",
            description="Test description",
            file_path="test/file.txt",
            status=JobStatus.PENDING
        )
        
        test_db_session.add(job)
        await test_db_session.commit()
        await test_db_session.refresh(job)
        
        assert job.id is not None
        assert job.name == "Test Job"
        assert job.status == JobStatus.PENDING
        assert job.created_at is not None
        assert job.progress_percentage == 0.0
    
    async def test_job_progress_calculation(self, test_db_session):
        """Test job progress calculation."""
        job = Job(
            name="Test Job",
            file_path="test/file.txt",
            total_urls=100,
            completed_urls=25,
            failed_urls=5
        )
        
        assert job.progress_percentage == 25.0
        
        job.completed_urls = 50
        assert job.progress_percentage == 50.0
        
        job.total_urls = 0  # Edge case
        assert job.progress_percentage == 0.0
    
    async def test_job_is_complete(self, test_db_session):
        """Test job completion status."""
        job = Job(name="Test", file_path="test.txt")
        
        job.status = JobStatus.PENDING
        assert not job.is_complete
        
        job.status = JobStatus.PROCESSING
        assert not job.is_complete
        
        job.status = JobStatus.COMPLETED
        assert job.is_complete
        
        job.status = JobStatus.FAILED
        assert job.is_complete
        
        job.status = JobStatus.CANCELLED
        assert job.is_complete


class TestTaskModel:
    """Test Task database model."""
    
    async def test_create_task(self, test_db_session):
        """Test creating a new task."""
        # First create a job
        job = Job(name="Test Job", file_path="test.txt")
        test_db_session.add(job)
        await test_db_session.flush()
        
        task = Task(
            job_id=job.id,
            url="https://example.com",
            status=TaskStatus.PENDING,
            worker_type=WorkerType.HTTP
        )
        
        test_db_session.add(task)
        await test_db_session.commit()
        await test_db_session.refresh(task)
        
        assert task.id is not None
        assert task.job_id == job.id
        assert task.url == "https://example.com"
        assert task.status == TaskStatus.PENDING
        assert task.worker_type == WorkerType.HTTP
        assert task.retry_count == 0
    
    async def test_task_can_retry(self, test_db_session):
        """Test task retry logic."""
        job = Job(name="Test Job", file_path="test.txt")
        test_db_session.add(job)
        await test_db_session.flush()
        
        task = Task(
            job_id=job.id,
            url="https://example.com",
            status=TaskStatus.FAILED,
            retry_count=2,
            max_retries=3
        )
        
        assert task.can_retry  # 2 < 3
        
        task.retry_count = 3
        assert not task.can_retry  # 3 >= 3
        
        task.status = TaskStatus.COMPLETED
        assert not task.can_retry  # Not failed


class TestResultModel:
    """Test Result database model."""
    
    async def test_create_result(self, test_db_session):
        """Test creating a scraping result."""
        # Create job and task first
        job = Job(name="Test Job", file_path="test.txt")
        test_db_session.add(job)
        await test_db_session.flush()
        
        task = Task(
            job_id=job.id,
            url="https://example.com",
            status=TaskStatus.COMPLETED,
            worker_type=WorkerType.HTTP
        )
        test_db_session.add(task)
        await test_db_session.flush()
        
        result = Result(
            task_id=task.id,
            url="https://example.com",
            title="Example Page",
            content="Example content",
            links=["https://example.com/link1", "https://example.com/link2"],
            images=["https://example.com/image1.jpg"],
            emails=["test@example.com"],
            content_quality_score=0.8,
            data_completeness=0.9
        )
        
        test_db_session.add(result)
        await test_db_session.commit()
        await test_db_session.refresh(result)
        
        assert result.id is not None
        assert result.task_id == task.id
        assert result.title == "Example Page"
        assert len(result.links) == 2
        assert len(result.images) == 1
        assert len(result.emails) == 1
        assert result.content_quality_score == 0.8


class TestDatabaseRelationships:
    """Test database model relationships."""
    
    async def test_job_tasks_relationship(self, test_db_session):
        """Test job-tasks relationship."""
        job = Job(name="Test Job", file_path="test.txt")
        test_db_session.add(job)
        await test_db_session.flush()
        
        # Create multiple tasks for the job
        tasks = []
        for i in range(3):
            task = Task(
                job_id=job.id,
                url=f"https://example.com/page{i}",
                status=TaskStatus.PENDING,
                worker_type=WorkerType.HTTP
            )
            tasks.append(task)
            test_db_session.add(task)
        
        await test_db_session.commit()
        
        # Query job with tasks
        stmt = select(Job).where(Job.id == job.id)
        result = await test_db_session.execute(stmt)
        job_with_tasks = result.scalar_one()
        
        # Access tasks through relationship
        await test_db_session.refresh(job_with_tasks, ['tasks'])
        assert len(job_with_tasks.tasks) == 3
    
    async def test_task_result_relationship(self, test_db_session):
        """Test task-result relationship."""
        job = Job(name="Test Job", file_path="test.txt")
        test_db_session.add(job)
        await test_db_session.flush()
        
        task = Task(
            job_id=job.id,
            url="https://example.com",
            status=TaskStatus.COMPLETED,
            worker_type=WorkerType.HTTP
        )
        test_db_session.add(task)
        await test_db_session.flush()
        
        result = Result(
            task_id=task.id,
            url="https://example.com",
            title="Test Result"
        )
        test_db_session.add(result)
        await test_db_session.commit()
        
        # Query task with result
        stmt = select(Task).where(Task.id == task.id)
        query_result = await test_db_session.execute(stmt)
        task_with_result = query_result.scalar_one()
        
        # Access result through relationship
        await test_db_session.refresh(task_with_result, ['result'])
        assert task_with_result.result is not None
        assert task_with_result.result.title == "Test Result"
    
    async def test_cascade_delete(self, test_db_session):
        """Test cascade delete behavior."""
        job = Job(name="Test Job", file_path="test.txt")
        test_db_session.add(job)
        await test_db_session.flush()
        
        task = Task(
            job_id=job.id,
            url="https://example.com",
            status=TaskStatus.COMPLETED,
            worker_type=WorkerType.HTTP
        )
        test_db_session.add(task)
        await test_db_session.flush()
        
        result = Result(
            task_id=task.id,
            url="https://example.com",
            title="Test Result"
        )
        test_db_session.add(result)
        await test_db_session.commit()
        
        # Delete job should cascade to tasks and results
        await test_db_session.delete(job)
        await test_db_session.commit()
        
        # Verify task and result are also deleted
        task_stmt = select(Task).where(Task.id == task.id)
        task_result = await test_db_session.execute(task_stmt)
        assert task_result.scalar_one_or_none() is None
        
        result_stmt = select(Result).where(Result.id == result.id)
        result_query = await test_db_session.execute(result_stmt)
        assert result_query.scalar_one_or_none() is None