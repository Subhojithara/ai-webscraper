"""
Integration tests for the complete K-Scrape Nexus system.
"""

import pytest
import asyncio
import uuid
import json
from datetime import datetime, timezone
from unittest.mock import patch, AsyncMock, MagicMock

from scraper_lib.database import Job, Task, JobStatus, TaskStatus, WorkerType
from services.ingestion_service.app.services.ingestion_service import IngestionService
from services.http_scraper_worker.app.services.http_scraper import HTTPScraper


class TestJobIngestionFlow:
    """Test complete job ingestion and task creation flow."""
    
    async def test_complete_ingestion_flow(self, test_db_session, test_redis_client, sample_urls):
        """Test complete flow from job creation to task generation."""
        # Create a job
        job = Job(
            name="Integration Test Job",
            description="Test job for integration testing",
            file_path="test/sample-urls.txt",
            status=JobStatus.PENDING
        )
        
        test_db_session.add(job)
        await test_db_session.commit()
        await test_db_session.refresh(job)
        
        # Mock file content in S3
        file_content = "\n".join(sample_urls)
        
        with patch('services.ingestion_service.app.services.s3_client.S3Client') as mock_s3:
            mock_s3_instance = AsyncMock()
            mock_s3_instance.download_file.return_value = file_content.encode('utf-8')
            mock_s3.return_value = mock_s3_instance
            
            # Create ingestion service
            ingestion_service = IngestionService()
            
            # Process the job
            await ingestion_service.ingest_file(job.id)
        
        # Verify job was updated
        await test_db_session.refresh(job)
        assert job.status == JobStatus.PROCESSING
        assert job.total_urls == len(sample_urls)
        assert job.started_at is not None
        
        # Verify tasks were created
        from sqlalchemy import select
        stmt = select(Task).where(Task.job_id == job.id)
        result = await test_db_session.execute(stmt)
        tasks = result.scalars().all()
        
        assert len(tasks) == len(sample_urls)
        
        # Verify all tasks have correct properties
        for task in tasks:
            assert task.job_id == job.id
            assert task.status == TaskStatus.PENDING
            assert task.worker_type in [WorkerType.HTTP, WorkerType.HEADLESS]
            assert task.url in sample_urls
    
    async def test_csv_file_ingestion(self, test_db_session, sample_csv_content):
        """Test ingestion of CSV file format."""
        job = Job(
            name="CSV Test Job",
            file_path="test/sample-urls.csv",
            status=JobStatus.PENDING
        )
        
        test_db_session.add(job)
        await test_db_session.commit()
        await test_db_session.refresh(job)
        
        with patch('services.ingestion_service.app.services.s3_client.S3Client') as mock_s3:
            mock_s3_instance = AsyncMock()
            mock_s3_instance.download_file.return_value = sample_csv_content.encode('utf-8')
            mock_s3.return_value = mock_s3_instance
            
            ingestion_service = IngestionService()
            await ingestion_service.ingest_file(job.id)
        
        # Verify results
        await test_db_session.refresh(job)
        assert job.status == JobStatus.PROCESSING
        assert job.total_urls == 3  # 3 URLs in sample CSV
    
    async def test_json_file_ingestion(self, test_db_session, sample_json_content):
        """Test ingestion of JSON file format."""
        job = Job(
            name="JSON Test Job",
            file_path="test/sample-urls.json",
            status=JobStatus.PENDING
        )
        
        test_db_session.add(job)
        await test_db_session.commit()
        await test_db_session.refresh(job)
        
        json_content = json.dumps(sample_json_content)
        
        with patch('services.ingestion_service.app.services.s3_client.S3Client') as mock_s3:
            mock_s3_instance = AsyncMock()
            mock_s3_instance.download_file.return_value = json_content.encode('utf-8')
            mock_s3.return_value = mock_s3_instance
            
            ingestion_service = IngestionService()
            await ingestion_service.ingest_file(job.id)
        
        # Verify results
        await test_db_session.refresh(job)
        assert job.status == JobStatus.PROCESSING
        assert job.total_urls == len(sample_json_content["urls"])


class TestScrapingWorkflow:
    """Test HTTP scraping workflow."""
    
    async def test_http_scraping_success(self, test_db_session, sample_html_content):
        """Test successful HTTP scraping with content extraction."""
        # Create job and task
        job = Job(
            name="Scraping Test Job",
            file_path="test/test.txt",
            status=JobStatus.PROCESSING,
            total_urls=1
        )
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
        
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_html_content
        mock_response.content = sample_html_content.encode('utf-8')
        mock_response.headers = {"content-type": "text/html"}
        mock_response.url = "https://example.com"
        mock_response.history = []
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            with patch('services.http_scraper_worker.app.services.proxy_manager.ProxyManager') as mock_proxy:
                mock_proxy_instance = AsyncMock()
                mock_proxy_instance.initialize = AsyncMock()
                mock_proxy.return_value = mock_proxy_instance
                
                # Create scraper
                scraper = HTTPScraper(mock_proxy_instance)
                await scraper.initialize_client()
                
                # Mock queue message
                message = MagicMock()
                message.id = "test-message-1"
                message.data = {
                    "task_id": str(task.id),
                    "job_id": str(job.id),
                    "url": "https://example.com",
                    "config": {}
                }
                message.retry_count = 0
                message.max_retries = 3
                
                # Process scraping task
                await scraper.process_scraping_task(message, "test-worker")
        
        # Verify task was updated
        await test_db_session.refresh(task)
        assert task.status == TaskStatus.COMPLETED
        assert task.completed_at is not None
        assert task.duration_seconds is not None
        assert task.status_code == 200
        
        # Verify result was stored
        from sqlalchemy import select
        from scraper_lib.database import Result
        stmt = select(Result).where(Result.task_id == task.id)
        result = await test_db_session.execute(stmt)
        scraping_result = result.scalar_one_or_none()
        
        assert scraping_result is not None
        assert scraping_result.url == "https://example.com"
        assert scraping_result.title == "Test Page"
        assert "Test content" in scraping_result.content
        assert len(scraping_result.links) > 0
        assert len(scraping_result.emails) > 0  # Should extract test@example.com
        assert len(scraping_result.phone_numbers) > 0  # Should extract (555) 123-4567
    
    async def test_http_scraping_failure(self, test_db_session):
        """Test HTTP scraping failure handling."""
        # Create job and task
        job = Job(
            name="Scraping Failure Test",
            file_path="test/test.txt",
            status=JobStatus.PROCESSING,
            total_urls=1
        )
        test_db_session.add(job)
        await test_db_session.flush()
        
        task = Task(
            job_id=job.id,
            url="https://invalid-domain-that-does-not-exist.com",
            status=TaskStatus.PENDING,
            worker_type=WorkerType.HTTP
        )
        test_db_session.add(task)
        await test_db_session.commit()
        await test_db_session.refresh(task)
        
        # Mock HTTP failure
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get.side_effect = Exception("Connection failed")
            mock_client_class.return_value = mock_client
            
            with patch('services.http_scraper_worker.app.services.proxy_manager.ProxyManager') as mock_proxy:
                mock_proxy_instance = AsyncMock()
                mock_proxy_instance.initialize = AsyncMock()
                mock_proxy.return_value = mock_proxy_instance
                
                scraper = HTTPScraper(mock_proxy_instance)
                await scraper.initialize_client()
                
                message = MagicMock()
                message.id = "test-message-2"
                message.data = {
                    "task_id": str(task.id),
                    "job_id": str(job.id),
                    "url": "https://invalid-domain-that-does-not-exist.com",
                    "config": {}
                }
                message.retry_count = 3  # Max retries reached
                message.max_retries = 3
                
                # Process scraping task (should fail)
                await scraper.process_scraping_task(message, "test-worker")
        
        # Verify task was marked as failed
        await test_db_session.refresh(task)
        assert task.status == TaskStatus.FAILED
        assert task.error_message is not None
        assert "Connection failed" in task.error_message


class TestDatabaseIntegration:
    """Test database operations and relationships."""
    
    async def test_job_task_relationship(self, test_db_session):
        """Test job-task relationship and cascade operations."""
        # Create job with multiple tasks
        job = Job(
            name="Relationship Test Job",
            file_path="test/test.txt",
            status=JobStatus.PROCESSING,
            total_urls=3
        )
        test_db_session.add(job)
        await test_db_session.flush()
        
        tasks = []
        for i in range(3):
            task = Task(
                job_id=job.id,
                url=f"https://example{i}.com",
                status=TaskStatus.PENDING,
                worker_type=WorkerType.HTTP
            )
            tasks.append(task)
            test_db_session.add(task)
        
        await test_db_session.commit()
        
        # Verify relationships
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        
        stmt = select(Job).options(selectinload(Job.tasks)).where(Job.id == job.id)
        result = await test_db_session.execute(stmt)
        job_with_tasks = result.scalar_one()
        
        assert len(job_with_tasks.tasks) == 3
        for task in job_with_tasks.tasks:
            assert task.job_id == job.id
    
    async def test_concurrent_task_updates(self, test_db_session):
        """Test concurrent task status updates."""
        # Create job and tasks
        job = Job(
            name="Concurrent Test Job",
            file_path="test/test.txt",
            status=JobStatus.PROCESSING,
            total_urls=5
        )
        test_db_session.add(job)
        await test_db_session.flush()
        
        tasks = []
        for i in range(5):
            task = Task(
                job_id=job.id,
                url=f"https://example{i}.com",
                status=TaskStatus.PENDING,
                worker_type=WorkerType.HTTP
            )
            tasks.append(task)
            test_db_session.add(task)
        
        await test_db_session.commit()
        
        # Simulate concurrent updates
        async def update_task_status(task_id, status):
            async for db in test_db_session:
                from sqlalchemy import select
                stmt = select(Task).where(Task.id == task_id)
                result = await db.execute(stmt)
                task = result.scalar_one()
                task.status = status
                await db.commit()
        
        # Update all tasks concurrently
        update_tasks = [
            update_task_status(task.id, TaskStatus.COMPLETED) 
            for task in tasks
        ]
        
        await asyncio.gather(*update_tasks)
        
        # Verify all tasks were updated
        from sqlalchemy import select
        stmt = select(Task).where(Task.job_id == job.id)
        result = await test_db_session.execute(stmt)
        updated_tasks = result.scalars().all()
        
        for task in updated_tasks:
            assert task.status == TaskStatus.COMPLETED


class TestRedisIntegration:
    """Test Redis queue operations and message flow."""
    
    async def test_queue_message_flow(self, test_redis_client):
        """Test complete message flow through Redis queues."""
        from scraper_lib.cache import RedisQueue, QueueMessage
        from datetime import datetime
        
        queue = RedisQueue(test_redis_client, "test_queue")
        
        # Setup consumer group
        await queue.setup_consumer_group("test_consumer")
        
        # Publish message
        test_data = {
            "task_id": str(uuid.uuid4()),
            "job_id": str(uuid.uuid4()),
            "url": "https://example.com"
        }
        
        message_id = await queue.publish(test_data, priority=1)
        assert message_id is not None
        
        # Consume message
        messages = await queue.consume("test_consumer", timeout=1000, count=1)
        assert len(messages) == 1
        
        consumed_message = messages[0]
        assert consumed_message.data["task_id"] == test_data["task_id"]
        assert consumed_message.data["url"] == test_data["url"]
        
        # Acknowledge message
        ack_result = await queue.acknowledge("test_consumer", consumed_message.id)
        assert ack_result == 1
        
        # Verify queue is empty
        queue_length = await queue.get_queue_length()
        assert queue_length == 0
    
    async def test_queue_retry_mechanism(self, test_redis_client):
        """Test message retry mechanism."""
        from scraper_lib.cache import RedisQueue, QueueMessage
        
        queue = RedisQueue(test_redis_client, "retry_test_queue")
        await queue.setup_consumer_group("retry_consumer")
        
        # Publish message
        test_data = {"test": "retry_data"}
        message_id = await queue.publish(test_data)
        
        # Consume message (simulate failure)
        messages = await queue.consume("retry_consumer", timeout=1000, count=1)
        message = messages[0]
        
        # Don't acknowledge (simulate processing failure)
        # Re-publish for retry
        message.retry_count += 1
        retry_message_id = await queue.publish(message.data)
        
        # Consume retry message
        retry_messages = await queue.consume("retry_consumer", timeout=1000, count=1)
        assert len(retry_messages) == 1
        
        retry_message = retry_messages[0]
        assert retry_message.data == test_data


class TestEndToEndWorkflow:
    """Test complete end-to-end scraping workflow."""
    
    async def test_complete_scraping_workflow(self, test_db_session, test_redis_client):
        """Test complete workflow from job creation to result extraction."""
        # This test simulates the entire pipeline
        # Note: This is a simplified version - a real E2E test would involve
        # starting actual services and testing with real HTTP requests
        
        # 1. Create job
        job = Job(
            name="E2E Test Job",
            file_path="test/e2e-test.txt",
            status=JobStatus.PENDING,
            config={"timeout": 30, "retries": 3}
        )
        test_db_session.add(job)
        await test_db_session.commit()
        await test_db_session.refresh(job)
        
        # 2. Simulate ingestion process
        urls = ["https://example.com", "https://test.com"]
        
        # Create tasks
        tasks = []
        for url in urls:
            task = Task(
                job_id=job.id,
                url=url,
                status=TaskStatus.PENDING,
                worker_type=WorkerType.HTTP,
                config=job.config
            )
            tasks.append(task)
            test_db_session.add(task)
        
        # Update job
        job.status = JobStatus.PROCESSING
        job.total_urls = len(urls)
        job.started_at = datetime.now(timezone.utc)
        
        await test_db_session.commit()
        
        # 3. Simulate scraping results
        from scraper_lib.database import Result
        
        for task in tasks:
            # Update task as completed
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc)
            task.duration_seconds = 2.5
            task.status_code = 200
            
            # Create result
            result = Result(
                task_id=task.id,
                url=task.url,
                title=f"Page Title for {task.url}",
                content=f"Sample content from {task.url}",
                links=[f"{task.url}/link1", f"{task.url}/link2"],
                images=[f"{task.url}/image1.jpg"],
                content_quality_score=0.8,
                data_completeness=0.9,
                metadata={"scraped_at": datetime.now(timezone.utc).isoformat()}
            )
            test_db_session.add(result)
        
        # Update job status
        job.status = JobStatus.COMPLETED
        job.completed_urls = len(urls)
        job.completed_at = datetime.now(timezone.utc)
        
        await test_db_session.commit()
        
        # 4. Verify final state
        await test_db_session.refresh(job)
        assert job.status == JobStatus.COMPLETED
        assert job.progress_percentage == 100.0
        assert job.completed_urls == len(urls)
        assert job.failed_urls == 0
        
        # Verify all tasks completed
        from sqlalchemy import select
        stmt = select(Task).where(Task.job_id == job.id)
        result = await test_db_session.execute(stmt)
        final_tasks = result.scalars().all()
        
        for task in final_tasks:
            assert task.status == TaskStatus.COMPLETED
            assert task.completed_at is not None
        
        # Verify results exist
        stmt = select(Result).join(Task).where(Task.job_id == job.id)
        result = await test_db_session.execute(stmt)
        results = result.scalars().all()
        
        assert len(results) == len(urls)
        for result in results:
            assert result.title is not None
            assert result.content is not None
            assert result.content_quality_score > 0