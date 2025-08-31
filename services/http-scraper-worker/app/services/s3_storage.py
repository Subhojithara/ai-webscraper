"""
S3 storage service for storing raw scraped content.
"""

import uuid
import json
from datetime import datetime
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError
import structlog

from scraper_lib import get_settings
from scraper_lib.observability import monitor_function

logger = structlog.get_logger("http-scraper-worker.s3_storage")


class S3Storage:
    """S3 storage service for raw scraped content."""
    
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
    
    @monitor_function("store_raw_content")
    async def store_raw_content(
        self, 
        html_content: str, 
        url: str, 
        content_type: str = "text/html"
    ) -> str:
        """Store raw HTML content in S3."""
        try:
            # Generate file path
            file_path = self.generate_content_path(url)
            
            # Prepare content with metadata
            content_data = {
                "url": url,
                "scraped_at": datetime.utcnow().isoformat(),
                "content_type": content_type,
                "html": html_content
            }
            
            # Store as JSON for easier processing later
            content_json = json.dumps(content_data, ensure_ascii=False, indent=2)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path,
                Body=content_json.encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'source_url': url,
                    'scraped_at': datetime.utcnow().isoformat(),
                    'content_size': str(len(html_content))
                }
            )
            
            logger.debug(
                "Raw content stored in S3",
                file_path=file_path,
                url=url,
                content_size=len(html_content)
            )
            
            return file_path
            
        except Exception as e:
            logger.error(
                "Failed to store raw content",
                url=url,
                error=str(e)
            )
            raise
    
    def generate_content_path(self, url: str) -> str:
        """Generate S3 path for storing content."""
        # Parse URL to get domain
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.replace('www.', '')
        
        # Generate timestamp-based path
        timestamp = datetime.utcnow()
        date_path = timestamp.strftime("%Y/%m/%d")
        
        # Generate unique filename
        file_id = str(uuid.uuid4())
        filename = f"{file_id}.json"
        
        # Construct full path
        path = f"raw_data/{domain}/{date_path}/{filename}"
        
        return path
    
    @monitor_function("get_raw_content")
    async def get_raw_content(self, file_path: str) -> dict:
        """Retrieve raw content from S3."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            logger.debug("Raw content retrieved from S3", file_path=file_path)
            
            return data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error("Raw content not found", file_path=file_path)
                raise FileNotFoundError(f"Content not found: {file_path}")
            else:
                logger.error("S3 retrieval failed", file_path=file_path, error=str(e))
                raise
    
    async def delete_raw_content(self, file_path: str) -> bool:
        """Delete raw content from S3."""
        try:
            self.s3_client.delete_object(
                Bucket=self.settings.s3_bucket,
                Key=file_path
            )
            
            logger.debug("Raw content deleted from S3", file_path=file_path)
            return True
            
        except Exception as e:
            logger.error("Failed to delete raw content", file_path=file_path, error=str(e))
            return False
    
    async def list_content_by_domain(self, domain: str, limit: int = 100) -> list:
        """List stored content for a specific domain."""
        try:
            prefix = f"raw_data/{domain}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.settings.s3_bucket,
                Prefix=prefix,
                MaxKeys=limit
            )
            
            contents = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    contents.append({
                        'key': obj['Key'],
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })
            
            return contents
            
        except Exception as e:
            logger.error("Failed to list content", domain=domain, error=str(e))
            return []