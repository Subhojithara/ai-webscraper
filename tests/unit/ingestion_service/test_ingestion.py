"""
Unit tests for Ingestion Service.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
import tempfile
import json
import csv
import io
from typing import List

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'services', 'ingestion-service'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'services', 'worker-shared'))

from app.services.ingestion_service import IngestionService
from scraper_lib.models import Job, Task, JobStatus, TaskStatus
from scraper_lib.queue import QueueClient


class TestIngestionService:
    """Test cases for IngestionService."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session."""
        session = AsyncMock()
        return session
    
    @pytest.fixture
    def mock_queue_client(self):
        """Mock queue client."""
        return Mock(spec=QueueClient)
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client."""
        client = AsyncMock()
        return client
    
    @pytest.fixture
    def ingestion_service(self, mock_db_session, mock_queue_client, mock_s3_client):
        """Create IngestionService instance with mocked dependencies."""
        return IngestionService(
            db_session=mock_db_session,
            queue_client=mock_queue_client,
            s3_client=mock_s3_client
        )
    
    @pytest.mark.asyncio
    async def test_process_txt_file(self, ingestion_service, mock_s3_client):
        """Test processing a text file with URLs."""
        # Setup
        job_id = "test-job-123"
        file_key = "uploads/test.txt"
        
        # Mock file content
        file_content = "https://example.com/page1\nhttps://example.com/page2\nhttps://example.com/page3"
        mock_s3_client.get_object.return_value = {
            'Body': io.BytesIO(file_content.encode())
        }
        
        # Execute
        urls = await ingestion_service._extract_urls_from_file(file_key, 'txt')
        
        # Verify
        assert len(urls) == 3
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
        assert "https://example.com/page3" in urls
        mock_s3_client.get_object.assert_called_once_with(Bucket='scraping-data', Key=file_key)
    
    @pytest.mark.asyncio
    async def test_process_csv_file(self, ingestion_service, mock_s3_client):
        """Test processing a CSV file with URLs."""
        # Setup
        file_key = "uploads/test.csv"
        
        # Mock CSV content
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        writer.writerow(['url', 'title'])
        writer.writerow(['https://example.com/page1', 'Page 1'])
        writer.writerow(['https://example.com/page2', 'Page 2'])
        
        mock_s3_client.get_object.return_value = {
            'Body': io.BytesIO(csv_data.getvalue().encode())
        }
        
        # Execute
        urls = await ingestion_service._extract_urls_from_file(file_key, 'csv')
        
        # Verify
        assert len(urls) == 2
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
    
    @pytest.mark.asyncio
    async def test_process_json_file(self, ingestion_service, mock_s3_client):
        """Test processing a JSON file with URLs."""
        # Setup
        file_key = "uploads/test.json"
        
        # Mock JSON content
        json_data = {
            "urls": [
                {"url": "https://example.com/page1", "priority": 1},
                {"url": "https://example.com/page2", "priority": 2}
            ]
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': io.BytesIO(json.dumps(json_data).encode())
        }
        
        # Execute
        urls = await ingestion_service._extract_urls_from_file(file_key, 'json')
        
        # Verify
        assert len(urls) == 2
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
    
    @pytest.mark.asyncio
    async def test_create_tasks_from_urls(self, ingestion_service, mock_db_session):
        """Test creating tasks from URL list."""
        # Setup
        job_id = "test-job-123"
        urls = ["https://example.com/page1", "https://example.com/page2", "https://example.com/page3"]
        
        # Execute
        await ingestion_service._create_tasks_from_urls(job_id, urls)
        
        # Verify
        assert mock_db_session.add.call_count == 3
        mock_db_session.commit.assert_called_once()
        
        # Verify task creation
        created_tasks = []
        for call in mock_db_session.add.call_args_list:
            task = call[0][0]
            created_tasks.append(task)
            assert isinstance(task, Task)
            assert task.job_id == job_id
            assert task.url in urls
            assert task.status == TaskStatus.PENDING
    
    @pytest.mark.asyncio
    async def test_enqueue_scraping_tasks(self, ingestion_service, mock_queue_client):
        """Test enqueueing scraping tasks."""
        # Setup
        tasks = [
            Task(id="task1", job_id="job1", url="https://example.com/page1"),
            Task(id="task2", job_id="job1", url="https://example.com/page2"),
        ]
        
        # Execute
        await ingestion_service._enqueue_scraping_tasks(tasks)
        
        # Verify
        assert mock_queue_client.enqueue.call_count == 2
        
        # Verify queue messages
        for call in mock_queue_client.enqueue.call_args_list:
            queue_name, message = call[0]
            assert queue_name == "scrape_http"
            assert "task_id" in message
            assert "url" in message
    
    @pytest.mark.asyncio
    async def test_update_job_status(self, ingestion_service, mock_db_session):
        """Test updating job status."""
        # Setup
        job_id = "test-job-123"
        task_count = 5
        
        # Mock job query
        mock_job = Mock(spec=Job)
        mock_db_session.execute.return_value.scalar_one_or_none.return_value = mock_job
        
        # Execute
        await ingestion_service._update_job_status(job_id, task_count)
        
        # Verify
        assert mock_job.total_tasks == task_count
        assert mock_job.status == JobStatus.PROCESSING
        mock_db_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_file_complete_workflow(self, ingestion_service, mock_s3_client, mock_db_session, mock_queue_client):
        """Test complete file processing workflow."""
        # Setup
        job_id = "test-job-123"
        file_key = "uploads/test.txt"
        file_type = "txt"
        
        # Mock file content
        file_content = "https://example.com/page1\nhttps://example.com/page2"
        mock_s3_client.get_object.return_value = {
            'Body': io.BytesIO(file_content.encode())
        }
        
        # Mock job query
        mock_job = Mock(spec=Job)
        mock_db_session.execute.return_value.scalar_one_or_none.return_value = mock_job
        
        # Execute
        await ingestion_service.process_file(job_id, file_key, file_type)
        
        # Verify S3 file reading
        mock_s3_client.get_object.assert_called_once()
        
        # Verify task creation
        assert mock_db_session.add.call_count == 2
        mock_db_session.commit.assert_called()
        
        # Verify queue enqueueing
        assert mock_queue_client.enqueue.call_count == 2
        
        # Verify job status update
        assert mock_job.total_tasks == 2
        assert mock_job.status == JobStatus.PROCESSING
    
    @pytest.mark.asyncio
    async def test_invalid_file_type(self, ingestion_service):
        """Test handling of invalid file types."""
        # Setup
        job_id = "test-job-123"
        file_key = "uploads/test.invalid"
        file_type = "invalid"
        
        # Execute & Verify
        with pytest.raises(ValueError, match="Unsupported file type"):
            await ingestion_service.process_file(job_id, file_key, file_type)
    
    @pytest.mark.asyncio
    async def test_empty_file_handling(self, ingestion_service, mock_s3_client, mock_db_session):
        """Test handling of empty files."""
        # Setup
        job_id = "test-job-123"
        file_key = "uploads/empty.txt"
        file_type = "txt"
        
        # Mock empty file
        mock_s3_client.get_object.return_value = {
            'Body': io.BytesIO(b"")
        }
        
        # Mock job query
        mock_job = Mock(spec=Job)
        mock_db_session.execute.return_value.scalar_one_or_none.return_value = mock_job
        
        # Execute
        await ingestion_service.process_file(job_id, file_key, file_type)
        
        # Verify no tasks created
        mock_db_session.add.assert_not_called()
        
        # Verify job status updated
        assert mock_job.total_tasks == 0
        assert mock_job.status == JobStatus.PROCESSING
    
    @pytest.mark.asyncio
    async def test_malformed_json_handling(self, ingestion_service, mock_s3_client):
        """Test handling of malformed JSON files."""
        # Setup
        file_key = "uploads/malformed.json"
        
        # Mock malformed JSON
        mock_s3_client.get_object.return_value = {
            'Body': io.BytesIO(b"invalid json content")
        }
        
        # Execute & Verify
        with pytest.raises(json.JSONDecodeError):
            await ingestion_service._extract_urls_from_file(file_key, 'json')
    
    @pytest.mark.asyncio
    async def test_url_validation(self, ingestion_service):
        """Test URL validation during extraction."""
        # Setup - URLs with various formats
        urls = [
            "https://example.com/valid",
            "http://example.com/also-valid",
            "invalid-url",
            "ftp://example.com/not-http",
            "https://",
            "",
            "  https://example.com/with-spaces  "
        ]
        
        # Execute
        valid_urls = ingestion_service._validate_urls(urls)
        
        # Verify only valid HTTP/HTTPS URLs are kept
        assert len(valid_urls) == 3
        assert "https://example.com/valid" in valid_urls
        assert "http://example.com/also-valid" in valid_urls
        assert "https://example.com/with-spaces" in valid_urls
    
    @pytest.mark.asyncio
    async def test_duplicate_url_handling(self, ingestion_service):
        """Test handling of duplicate URLs in file."""
        # Setup
        urls = [
            "https://example.com/page1",
            "https://example.com/page2",
            "https://example.com/page1",  # duplicate
            "https://example.com/page3",
            "https://example.com/page2",  # duplicate
        ]
        
        # Execute
        unique_urls = ingestion_service._remove_duplicates(urls)
        
        # Verify duplicates removed
        assert len(unique_urls) == 3
        assert "https://example.com/page1" in unique_urls
        assert "https://example.com/page2" in unique_urls
        assert "https://example.com/page3" in unique_urls