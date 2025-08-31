"""
Upload service for handling S3 file operations.
"""

import uuid
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import structlog

from scraper_lib import get_settings
from ..models import UploadUrlRequest, UploadUrlResponse

logger = structlog.get_logger("api-server.upload_service")


class UploadService:
    """Service for handling file uploads and S3 operations."""
    
    def __init__(self):
        self.settings = get_settings()
        self._s3_client = None
    
    @property
    def s3_client(self):
        """Lazy-loaded S3 client."""
        if self._s3_client is None:
            self._s3_client = boto3.client(
                's3',
                endpoint_url=self.settings.s3_endpoint_url,
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.s3_region
            )
        return self._s3_client
    
    async def generate_upload_url(self, upload_request: UploadUrlRequest) -> UploadUrlResponse:
        """Generate pre-signed URL for file upload."""
        logger.info("Generating upload URL", filename=upload_request.filename)
        
        # Generate unique file path
        file_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().strftime("%Y/%m/%d")
        file_path = f"uploads/{timestamp}/{file_id}_{upload_request.filename}"
        
        try:
            # Generate pre-signed URL
            expires_in = 3600  # 1 hour
            
            upload_url = self.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.settings.s3_bucket,
                    'Key': file_path,
                    'ContentType': upload_request.content_type
                },
                ExpiresIn=expires_in
            )
            
            logger.info(
                "Upload URL generated",
                filename=upload_request.filename,
                file_path=file_path
            )
            
            return UploadUrlResponse(
                upload_url=upload_url,
                file_path=file_path,
                expires_in=expires_in
            )
            
        except ClientError as e:
            logger.error("Failed to generate upload URL", error=str(e))
            raise ValueError(f"Failed to generate upload URL: {str(e)}")
    
    async def verify_upload(self, file_path: str) -> bool:
        """Verify that a file exists and is accessible in S3."""
        logger.info("Verifying file upload", file_path=file_path)
        
        try:
            # Check if file exists
            self.s3_client.head_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            logger.info("File upload verified", file_path=file_path)
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.warning("File not found", file_path=file_path)
                return False
            else:
                logger.error("Error verifying file", file_path=file_path, error=str(e))
                raise
    
    async def get_download_url(self, file_path: str, expires_in: int = 3600) -> str:
        """Generate pre-signed URL for file download."""
        logger.info("Generating download URL", file_path=file_path)
        
        try:
            download_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.settings.s3_bucket,
                    'Key': file_path
                },
                ExpiresIn=expires_in
            )
            
            logger.info("Download URL generated", file_path=file_path)
            return download_url
            
        except ClientError as e:
            logger.error("Failed to generate download URL", file_path=file_path, error=str(e))
            raise ValueError(f"Failed to generate download URL: {str(e)}")
    
    async def delete_file(self, file_path: str) -> bool:
        """Delete a file from S3."""
        logger.info("Deleting file", file_path=file_path)
        
        try:
            self.s3_client.delete_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            logger.info("File deleted", file_path=file_path)
            return True
            
        except ClientError as e:
            logger.error("Failed to delete file", file_path=file_path, error=str(e))
            return False