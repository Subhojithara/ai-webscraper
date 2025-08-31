"""
Unit tests for API server endpoints and services.
"""

import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from datetime import datetime, timezone

from services.api_server.app.models import JobCreateRequest, JobResponse
from services.api_server.app.services.job_service import JobService
from services.api_server.app.services.upload_service import UploadService


class TestJobEndpoints:
    """Test job-related API endpoints."""
    
    def test_create_job_success(self, test_client, sample_job_data):
        """Test successful job creation."""
        with patch('services.api_server.app.services.job_service.JobService') as mock_service:
            # Mock service response
            mock_job = MagicMock()
            mock_job.id = uuid.uuid4()
            mock_job.name = sample_job_data["name"]
            mock_job.status = "pending"
            
            mock_service_instance = AsyncMock()
            mock_service_instance.create_job.return_value = JobResponse.model_validate(mock_job)
            mock_service.return_value = mock_service_instance
            
            # Make request
            response = test_client.post("/api/v1/jobs/", json=sample_job_data)
            
            assert response.status_code == 201
            data = response.json()
            assert data["name"] == sample_job_data["name"]
            assert data["status"] == "pending"
    
    def test_create_job_invalid_data(self, test_client):
        """Test job creation with invalid data."""
        invalid_data = {
            "name": "",  # Empty name
            "file_path": "invalid/path"
        }
        
        response = test_client.post("/api/v1/jobs/", json=invalid_data)
        assert response.status_code == 422  # Validation error
    
    def test_get_job_success(self, test_client):
        """Test successful job retrieval."""
        job_id = str(uuid.uuid4())
        
        with patch('services.api_server.app.services.job_service.JobService') as mock_service:
            # Mock service response
            mock_job = MagicMock()
            mock_job.id = uuid.UUID(job_id)
            mock_job.name = "Test Job"
            mock_job.status = "completed"
            
            mock_service_instance = AsyncMock()
            mock_service_instance.get_job.return_value = JobResponse.model_validate(mock_job)
            mock_service.return_value = mock_service_instance
            
            # Make request
            response = test_client.get(f"/api/v1/jobs/{job_id}")
            
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == job_id
            assert data["name"] == "Test Job"
    
    def test_get_job_not_found(self, test_client):
        """Test job retrieval when job doesn't exist."""
        job_id = str(uuid.uuid4())
        
        with patch('services.api_server.app.services.job_service.JobService') as mock_service:
            mock_service_instance = AsyncMock()
            mock_service_instance.get_job.return_value = None
            mock_service.return_value = mock_service_instance
            
            response = test_client.get(f"/api/v1/jobs/{job_id}")
            assert response.status_code == 404
    
    def test_list_jobs(self, test_client):
        """Test job listing with pagination."""
        with patch('services.api_server.app.services.job_service.JobService') as mock_service:
            # Mock service response
            mock_jobs = []
            for i in range(3):
                mock_job = MagicMock()
                mock_job.id = uuid.uuid4()
                mock_job.name = f"Job {i}"
                mock_job.status = "pending"
                mock_jobs.append(JobResponse.model_validate(mock_job))
            
            mock_list_response = MagicMock()
            mock_list_response.jobs = mock_jobs
            mock_list_response.total = 3
            mock_list_response.page = 1
            mock_list_response.page_size = 20
            mock_list_response.has_next = False
            mock_list_response.has_previous = False
            
            mock_service_instance = AsyncMock()
            mock_service_instance.list_jobs.return_value = mock_list_response
            mock_service.return_value = mock_service_instance
            
            # Make request
            response = test_client.get("/api/v1/jobs/?page=1&page_size=20")
            
            assert response.status_code == 200
            data = response.json()
            assert len(data["jobs"]) == 3
            assert data["total"] == 3
            assert data["page"] == 1
    
    def test_delete_job_success(self, test_client):
        """Test successful job deletion."""
        job_id = str(uuid.uuid4())
        
        with patch('services.api_server.app.services.job_service.JobService') as mock_service:
            mock_service_instance = AsyncMock()
            mock_service_instance.delete_job.return_value = True
            mock_service.return_value = mock_service_instance
            
            response = test_client.delete(f"/api/v1/jobs/{job_id}")
            assert response.status_code == 204
    
    def test_cancel_job_success(self, test_client):
        """Test successful job cancellation."""
        job_id = str(uuid.uuid4())
        
        with patch('services.api_server.app.services.job_service.JobService') as mock_service:
            # Mock cancelled job response
            mock_job = MagicMock()
            mock_job.id = uuid.UUID(job_id)
            mock_job.name = "Test Job"
            mock_job.status = "cancelled"
            
            mock_service_instance = AsyncMock()
            mock_service_instance.cancel_job.return_value = JobResponse.model_validate(mock_job)
            mock_service.return_value = mock_service_instance
            
            response = test_client.post(f"/api/v1/jobs/{job_id}/cancel")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "cancelled"


class TestUploadEndpoints:
    """Test upload-related API endpoints."""
    
    def test_get_upload_url_success(self, test_client):
        """Test successful upload URL generation."""
        upload_request = {
            "filename": "test.txt",
            "content_type": "text/plain"
        }
        
        with patch('services.api_server.app.services.upload_service.UploadService') as mock_service:
            mock_response = MagicMock()
            mock_response.upload_url = "https://s3.amazonaws.com/bucket/test.txt?signature=..."
            mock_response.file_path = "uploads/2023/01/01/test.txt"
            mock_response.expires_in = 3600
            
            mock_service_instance = AsyncMock()
            mock_service_instance.generate_upload_url.return_value = mock_response
            mock_service.return_value = mock_service_instance
            
            response = test_client.post("/api/v1/upload/url", json=upload_request)
            
            assert response.status_code == 200
            data = response.json()
            assert "upload_url" in data
            assert "file_path" in data
            assert data["expires_in"] == 3600
    
    def test_get_upload_url_invalid_filename(self, test_client):
        """Test upload URL generation with invalid filename."""
        upload_request = {
            "filename": "test.exe",  # Invalid extension
            "content_type": "application/octet-stream"
        }
        
        response = test_client.post("/api/v1/upload/url", json=upload_request)
        assert response.status_code == 422  # Validation error
    
    def test_verify_upload_success(self, test_client):
        """Test successful upload verification."""
        file_path = "uploads/test/test.txt"
        
        with patch('services.api_server.app.services.upload_service.UploadService') as mock_service:
            mock_service_instance = AsyncMock()
            mock_service_instance.verify_upload.return_value = True
            mock_service.return_value = mock_service_instance
            
            response = test_client.post(
                "/api/v1/upload/verify",
                params={"file_path": file_path}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "verified"
            assert data["file_path"] == file_path
    
    def test_verify_upload_not_found(self, test_client):
        """Test upload verification when file not found."""
        file_path = "uploads/test/nonexistent.txt"
        
        with patch('services.api_server.app.services.upload_service.UploadService') as mock_service:
            mock_service_instance = AsyncMock()
            mock_service_instance.verify_upload.return_value = False
            mock_service.return_value = mock_service_instance
            
            response = test_client.post(
                "/api/v1/upload/verify",
                params={"file_path": file_path}
            )
            
            assert response.status_code == 404


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_health_check_success(self, test_client):
        """Test successful health check."""
        with patch('scraper_lib.observability.get_health_checker') as mock_checker:
            mock_checker_instance = MagicMock()
            mock_checker_instance.run_checks.return_value = {
                "status": "healthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "checks": {
                    "database": {"status": "healthy"},
                    "redis": {"status": "healthy"}
                }
            }
            mock_checker.return_value = mock_checker_instance
            
            response = test_client.get("/health/")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert "checks" in data
    
    def test_readiness_check_success(self, test_client):
        """Test successful readiness check."""
        with patch('scraper_lib.observability.get_health_checker') as mock_checker:
            mock_checker_instance = MagicMock()
            mock_checker_instance.run_checks.return_value = {
                "status": "healthy"
            }
            mock_checker.return_value = mock_checker_instance
            
            response = test_client.get("/health/ready")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ready"
    
    def test_readiness_check_unhealthy(self, test_client):
        """Test readiness check when service is unhealthy."""
        with patch('scraper_lib.observability.get_health_checker') as mock_checker:
            mock_checker_instance = MagicMock()
            mock_checker_instance.run_checks.return_value = {
                "status": "unhealthy"
            }
            mock_checker.return_value = mock_checker_instance
            
            response = test_client.get("/health/ready")
            assert response.status_code == 503
    
    def test_liveness_check(self, test_client):
        """Test liveness check."""
        response = test_client.get("/health/live")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "alive"
        assert "timestamp" in data


class TestJobService:
    """Test JobService business logic."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session."""
        return AsyncMock()
    
    @pytest.fixture
    def job_service(self, mock_db_session):
        """Create JobService instance with mocked dependencies."""
        return JobService(mock_db_session)
    
    async def test_create_job_success(self, job_service, mock_db_session, sample_job_data):
        """Test successful job creation in service."""
        job_request = JobCreateRequest(**sample_job_data)
        
        # Mock database operations
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.name = sample_job_data["name"]
        mock_job.status = "pending"
        
        mock_db_session.add = MagicMock()
        mock_db_session.flush = AsyncMock()
        mock_db_session.refresh = AsyncMock()
        
        # Execute
        with patch('scraper_lib.database.Job') as mock_job_class:
            mock_job_class.return_value = mock_job
            result = await job_service.create_job(job_request)
        
        # Verify
        assert result.name == sample_job_data["name"]
        mock_db_session.add.assert_called_once()
        mock_db_session.flush.assert_called_once()
    
    async def test_get_job_success(self, job_service, mock_db_session):
        """Test successful job retrieval in service."""
        job_id = uuid.uuid4()
        
        # Mock database query
        mock_job = MagicMock()
        mock_job.id = job_id
        mock_job.name = "Test Job"
        
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_job
        mock_db_session.execute.return_value = mock_result
        
        # Execute
        result = await job_service.get_job(job_id)
        
        # Verify
        assert result.name == "Test Job"
        mock_db_session.execute.assert_called_once()
    
    async def test_get_job_not_found(self, job_service, mock_db_session):
        """Test job retrieval when job doesn't exist."""
        job_id = uuid.uuid4()
        
        # Mock database query returning None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db_session.execute.return_value = mock_result
        
        # Execute
        result = await job_service.get_job(job_id)
        
        # Verify
        assert result is None


class TestUploadService:
    """Test UploadService business logic."""
    
    @pytest.fixture
    def upload_service(self):
        """Create UploadService instance."""
        return UploadService()
    
    async def test_generate_upload_url_success(self, upload_service, mock_s3_client):
        """Test successful upload URL generation."""
        from services.api_server.app.models import UploadUrlRequest
        
        upload_request = UploadUrlRequest(
            filename="test.txt",
            content_type="text/plain"
        )
        
        # Mock S3 client
        upload_service._s3_client = mock_s3_client
        
        # Execute
        result = await upload_service.generate_upload_url(upload_request)
        
        # Verify
        assert result.upload_url.startswith("https://")
        assert result.file_path.endswith("test.txt")
        assert result.expires_in == 3600
        mock_s3_client.generate_presigned_url.assert_called_once()
    
    async def test_verify_upload_success(self, upload_service, mock_s3_client):
        """Test successful upload verification."""
        file_path = "uploads/test/test.txt"
        
        # Mock S3 client
        upload_service._s3_client = mock_s3_client
        
        # Execute
        result = await upload_service.verify_upload(file_path)
        
        # Verify
        assert result is True
        mock_s3_client.head_object.assert_called_once_with(
            Bucket=upload_service.settings.s3_bucket,
            Key=file_path
        )