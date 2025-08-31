"""
S3 storage service for Crawl4AI scraper results.
Reuses the same S3 storage pattern from other workers.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError
import structlog

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from scraper_lib import get_settings

logger = structlog.get_logger("crawl4ai-s3-storage")


class S3Storage:
    """S3 storage service for Crawl4AI scraping results."""
    
    def __init__(self):
        self.settings = get_settings()
        self.s3_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize S3 client with proper configuration."""
        try:
            if self.settings.s3_endpoint_url:
                # For MinIO or custom S3-compatible storage
                self.s3_client = boto3.client(
                    's3',
                    endpoint_url=self.settings.s3_endpoint_url,
                    aws_access_key_id=self.settings.s3_access_key,
                    aws_secret_access_key=self.settings.s3_secret_key,
                    region_name=self.settings.s3_region
                )
            else:
                # For AWS S3
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.settings.s3_access_key,
                    aws_secret_access_key=self.settings.s3_secret_key,
                    region_name=self.settings.s3_region
                )
            
            logger.info("S3 client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    async def store_result(
        self,
        job_id: uuid.UUID,
        task_id: uuid.UUID,
        result_data: Dict[str, Any]
    ) -> str:
        """
        Store Crawl4AI scraping result in S3.
        
        Args:
            job_id: Job UUID
            task_id: Task UUID
            result_data: Scraped data to store
            
        Returns:
            S3 object key/path
        """
        try:
            # Generate S3 key with organized structure
            timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
            s3_key = f"crawl4ai-results/{timestamp}/{job_id}/{task_id}.json"
            
            # Prepare data for storage
            storage_data = {
                "job_id": str(job_id),
                "task_id": str(task_id),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "extraction_type": result_data.get("extraction_type", "crawl4ai"),
                "result": result_data
            }
            
            # Store in S3
            self.s3_client.put_object(
                Bucket=self.settings.s3_bucket_name,
                Key=s3_key,
                Body=json.dumps(storage_data, indent=2),
                ContentType='application/json',
                ServerSideEncryption='AES256',
                Metadata={
                    'job-id': str(job_id),
                    'task-id': str(task_id),
                    'extraction-type': result_data.get("extraction_type", "crawl4ai"),
                    'quality-score': str(result_data.get("quality_score", 0)),
                    'content-size': str(result_data.get("content_size", 0))
                }
            )
            
            logger.info(
                "Crawl4AI result stored in S3",
                s3_key=s3_key,
                job_id=str(job_id),
                task_id=str(task_id),
                size=len(json.dumps(storage_data))
            )
            
            return s3_key
            
        except ClientError as e:
            logger.error(
                "S3 storage failed",
                error=str(e),
                job_id=str(job_id),
                task_id=str(task_id)
            )
            raise
    
    async def store_screenshot(
        self,
        job_id: uuid.UUID,
        task_id: uuid.UUID,
        screenshot_data: bytes,
        format: str = "png"
    ) -> str:
        """
        Store screenshot in S3.
        
        Args:
            job_id: Job UUID
            task_id: Task UUID
            screenshot_data: Raw screenshot bytes
            format: Image format (png, jpg, etc.)
            
        Returns:
            S3 object key/path for screenshot
        """
        try:
            # Generate S3 key for screenshot
            timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
            s3_key = f"crawl4ai-screenshots/{timestamp}/{job_id}/{task_id}.{format}"
            
            # Store screenshot in S3
            self.s3_client.put_object(
                Bucket=self.settings.s3_bucket_name,
                Key=s3_key,
                Body=screenshot_data,
                ContentType=f'image/{format}',
                ServerSideEncryption='AES256',
                Metadata={
                    'job-id': str(job_id),
                    'task-id': str(task_id),
                    'type': 'screenshot',
                    'format': format
                }
            )
            
            logger.info(
                "Screenshot stored in S3",
                s3_key=s3_key,
                job_id=str(job_id),
                task_id=str(task_id),
                size=len(screenshot_data)
            )
            
            return s3_key
            
        except ClientError as e:
            logger.error(
                "Screenshot S3 storage failed",
                error=str(e),
                job_id=str(job_id),
                task_id=str(task_id)
            )
            raise
    
    def get_result_url(self, s3_key: str, expires_in: int = 3600) -> str:
        """
        Generate presigned URL for accessing stored result.
        
        Args:
            s3_key: S3 object key
            expires_in: URL expiration time in seconds
            
        Returns:
            Presigned URL
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.settings.s3_bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=expires_in
            )
            return url
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise