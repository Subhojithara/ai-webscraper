"""
S3 client for handling file operations in the ingestion service.
"""

import boto3
from botocore.exceptions import ClientError
import structlog

from scraper_lib import get_settings
from scraper_lib.observability import monitor_function

logger = structlog.get_logger("ingestion-service.s3_client")


class S3Client:
    """S3 client for file operations."""
    
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
    
    @monitor_function("download_file")
    async def download_file(self, file_path: str) -> bytes:
        """Download file content from S3."""
        logger.info("Downloading file from S3", file_path=file_path)
        
        try:
            response = self.s3_client.get_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            content = response['Body'].read()
            
            logger.info(
                "File downloaded successfully",
                file_path=file_path,
                size=len(content)
            )
            
            return content
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error("File not found in S3", file_path=file_path)
                raise FileNotFoundError(f"File not found: {file_path}")
            else:
                logger.error("S3 download failed", file_path=file_path, error=str(e))
                raise
    
    @monitor_function("upload_file")
    async def upload_file(self, file_path: str, content: bytes, content_type: str = None) -> bool:
        """Upload file content to S3."""
        logger.info("Uploading file to S3", file_path=file_path, size=len(content))
        
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            # Upload using put_object for better control
            self.s3_client.put_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path,
                Body=content,
                **extra_args
            )
            
            logger.info("File uploaded successfully", file_path=file_path)
            return True
            
        except ClientError as e:
            logger.error("S3 upload failed", file_path=file_path, error=str(e))
            return False
    
    async def file_exists(self, file_path: str) -> bool:
        """Check if file exists in S3."""
        try:
            self.s3_client.head_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    async def get_file_metadata(self, file_path: str) -> dict:
        """Get file metadata from S3."""
        try:
            response = self.s3_client.head_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            return {
                'size': response.get('ContentLength', 0),
                'content_type': response.get('ContentType'),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }
            
        except ClientError as e:
            logger.error("Failed to get file metadata", file_path=file_path, error=str(e))
            raise
    
    async def delete_file(self, file_path: str) -> bool:
        """Delete file from S3."""
        logger.info("Deleting file from S3", file_path=file_path)
        
        try:
            self.s3_client.delete_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            logger.info("File deleted successfully", file_path=file_path)
            return True
            
        except ClientError as e:
            logger.error("S3 delete failed", file_path=file_path, error=str(e))
            return False