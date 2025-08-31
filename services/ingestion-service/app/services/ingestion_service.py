"""
Core ingestion service for processing file upload events and creating scraping tasks.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List
import structlog

from scraper_lib import get_settings, get_database_session, get_queue_manager
from scraper_lib.database import Job, Task, JobStatus, TaskStatus, WorkerType
from scraper_lib.observability import monitor_function
from scraper_lib.cache import QueueMessage, dequeue_job, ack_job
from .file_processor import FileProcessor
from .s3_client import S3Client


logger = structlog.get_logger("ingestion-service")


class IngestionService:
    """Main ingestion service for processing uploaded files and creating tasks."""
    
    def __init__(self):
        self.settings = get_settings()
        self.queue_manager = get_queue_manager()
        self.file_processor = FileProcessor()
        self.s3_client = S3Client()
        self.consumer_name = "ingestion-worker"
        self.running = False
    
    async def start_consumer(self):
        """Start consuming messages from the ingestion queue."""
        logger.info("Starting ingestion queue consumer", consumer=self.consumer_name)
        self.running = True
        
        while self.running:
            try:
                # Consume message from queue
                message = await dequeue_job(
                    self.settings.queue_new_files,
                    self.consumer_name,
                    timeout=5000
                )
                
                if message:
                    await self.process_message(message)
                    
            except Exception as e:
                logger.error("Error in consumer loop", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    def stop_consumer(self):
        """Stop the queue consumer."""
        logger.info("Stopping ingestion queue consumer")
        self.running = False
    
    @monitor_function("process_ingestion_message")
    async def process_message(self, message: QueueMessage):
        """Process a single ingestion message."""
        logger.info("Processing ingestion message", message_id=message.id, data=message.data)
        
        try:
            job_id = uuid.UUID(message.data["job_id"])
            action = message.data.get("action", "ingest_file")
            
            if action == "ingest_file":
                await self.ingest_file(job_id)
            else:
                logger.warning("Unknown action", action=action, job_id=str(job_id))
            
            # Acknowledge successful processing
            await ack_job(
                self.settings.queue_new_files,
                self.consumer_name,
                message.id
            )
            
            logger.info("Message processed successfully", message_id=message.id)
            
        except Exception as e:
            logger.error(
                "Failed to process message",
                message_id=message.id,
                error=str(e),
                retry_count=message.retry_count
            )
            
            # Handle retry logic
            if message.retry_count < message.max_retries:
                await self.retry_message(message)
            else:
                await self.handle_failed_message(message)
    
    @monitor_function("ingest_file")
    async def ingest_file(self, job_id: uuid.UUID):
        """Process a file and create scraping tasks."""
        logger.info("Starting file ingestion", job_id=str(job_id))
        
        async for db in get_database_session():
            try:
                # Get job details
                from sqlalchemy import select
                stmt = select(Job).where(Job.id == job_id)
                result = await db.execute(stmt)
                job = result.scalar_one_or_none()
                
                if not job:
                    raise ValueError(f"Job {job_id} not found")
                
                logger.info("Processing job file", job_id=str(job_id), file_path=job.file_path)
                
                # Update job status
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now(timezone.utc)
                job.updated_at = datetime.now(timezone.utc)
                
                # Download and process file
                file_content = await self.s3_client.download_file(job.file_path)
                urls = await self.file_processor.extract_urls_from_content(
                    file_content, 
                    job.file_path
                )
                
                logger.info(
                    "URLs extracted from file",
                    job_id=str(job_id),
                    url_count=len(urls)
                )
                
                # Create tasks for each URL
                tasks_created = await self.create_tasks(db, job, urls)
                
                # Update job with total URL count
                job.total_urls = len(urls)
                job.metadata = job.metadata or {}
                job.metadata.update({
                    "ingestion_completed_at": datetime.now(timezone.utc).isoformat(),
                    "tasks_created": tasks_created
                })
                
                await db.commit()
                
                logger.info(
                    "File ingestion completed",
                    job_id=str(job_id),
                    total_urls=len(urls),
                    tasks_created=tasks_created
                )
                
            except Exception as e:
                await db.rollback()
                
                # Update job status to failed
                try:
                    job.status = JobStatus.FAILED
                    job.completed_at = datetime.now(timezone.utc)
                    job.metadata = job.metadata or {}
                    job.metadata["ingestion_error"] = str(e)
                    await db.commit()
                except Exception:
                    pass
                
                logger.error("File ingestion failed", job_id=str(job_id), error=str(e))
                raise
    
    async def create_tasks(self, db, job: Job, urls: List[str]) -> int:
        """Create individual scraping tasks for URLs."""
        logger.info("Creating scraping tasks", job_id=str(job.id), url_count=len(urls))
        
        tasks_created = 0
        batch_size = 100
        
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            batch_tasks = []
            
            for url in batch_urls:
                # Determine worker type based on URL characteristics
                worker_type = self.determine_worker_type(url, job.config)
                
                task = Task(
                    job_id=job.id,
                    url=url,
                    status=TaskStatus.PENDING,
                    worker_type=worker_type,
                    priority=self.calculate_priority(url, job.config),
                    config=self.get_task_config(url, job.config),
                    metadata={"created_at": datetime.now(timezone.utc).isoformat()}
                )
                
                batch_tasks.append(task)
            
            # Add batch to database
            db.add_all(batch_tasks)
            await db.flush()
            
            # Enqueue tasks for processing
            await self.enqueue_tasks(batch_tasks)
            
            tasks_created += len(batch_tasks)
            
            logger.debug(
                "Task batch created",
                job_id=str(job.id),
                batch_size=len(batch_tasks),
                total_created=tasks_created
            )
        
        return tasks_created
    
    async def enqueue_tasks(self, tasks: List[Task]):
        """Enqueue tasks to appropriate worker queues."""
        for task in tasks:
            queue_name = self.get_queue_for_worker_type(task.worker_type)
            
            task_data = {
                "task_id": str(task.id),
                "job_id": str(task.job_id),
                "url": task.url,
                "worker_type": task.worker_type.value,
                "priority": task.priority,
                "config": task.config or {},
                "metadata": task.metadata or {}
            }
            
            await self.queue_manager.publish_to_queue(
                queue_name,
                task_data,
                priority=task.priority
            )
            
            logger.debug(
                "Task enqueued",
                task_id=str(task.id),
                queue=queue_name,
                worker_type=task.worker_type.value
            )
    
    def determine_worker_type(self, url: str, job_config: Dict[str, Any]) -> WorkerType:
        """Determine appropriate worker type for a URL."""
        # Check job-specific configuration
        if job_config and job_config.get("force_headless"):
            return WorkerType.HEADLESS
        
        # Check for JavaScript-heavy sites that need headless browser
        js_indicators = [
            "javascript:",
            "#",
            "angular",
            "react",
            "vue",
            "spa"
        ]
        
        url_lower = url.lower()
        for indicator in js_indicators:
            if indicator in url_lower:
                return WorkerType.HEADLESS
        
        # Check for file extensions that might need special handling
        doc_extensions = [".pdf", ".doc", ".docx", ".xls", ".xlsx"]
        for ext in doc_extensions:
            if url_lower.endswith(ext):
                return WorkerType.HTTP  # Special HTTP handling for documents
        
        # Default to HTTP worker
        return WorkerType.HTTP
    
    def calculate_priority(self, url: str, job_config: Dict[str, Any]) -> int:
        """Calculate task priority based on URL and job configuration."""
        base_priority = 1
        
        # High priority domains
        high_priority_domains = job_config.get("high_priority_domains", [])
        for domain in high_priority_domains:
            if domain in url:
                return 5
        
        # Medium priority for specific file types
        if any(ext in url.lower() for ext in [".pdf", ".doc", ".xls"]):
            return 3
        
        return base_priority
    
    def get_task_config(self, url: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Get task-specific configuration."""
        task_config = {}
        
        # Copy relevant job config to task
        if job_config:
            task_config.update({
                "timeout": job_config.get("timeout", 30),
                "retries": job_config.get("retries", 3),
                "user_agent": job_config.get("user_agent"),
                "headers": job_config.get("headers", {}),
                "follow_redirects": job_config.get("follow_redirects", True)
            })
        
        return task_config
    
    def get_queue_for_worker_type(self, worker_type: WorkerType) -> str:
        """Get queue name for worker type."""
        queue_mapping = {
            WorkerType.HTTP: self.settings.queue_scrape_http,
            WorkerType.HEADLESS: self.settings.queue_scrape_headless,
            WorkerType.CRAWL4AI: self.settings.queue_scrape_crawl4ai,
            WorkerType.PROCESSING: self.settings.queue_process_data
        }
        
        return queue_mapping.get(worker_type, self.settings.queue_scrape_http)
    
    async def retry_message(self, message: QueueMessage):
        """Retry a failed message."""
        logger.info("Retrying message", message_id=message.id, retry_count=message.retry_count)
        
        # Increment retry count
        message.retry_count += 1
        
        # Re-enqueue with delay
        await asyncio.sleep(min(2 ** message.retry_count, 60))  # Exponential backoff
        
        await self.queue_manager.publish_to_queue(
            self.settings.queue_new_files,
            message.data,
            priority=1
        )
    
    async def handle_failed_message(self, message: QueueMessage):
        """Handle a message that has exceeded retry limit."""
        logger.error(
            "Message failed permanently",
            message_id=message.id,
            retry_count=message.retry_count,
            data=message.data
        )
        
        # Move to dead letter queue
        await self.queue_manager.publish_to_queue(
            self.settings.queue_failed_tasks,
            {
                "original_message": message.to_dict(),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "service": "ingestion-service"
            }
        )
        
        # Update job status if possible
        try:
            job_id = uuid.UUID(message.data["job_id"])
            async for db in get_database_session():
                from sqlalchemy import select
                stmt = select(Job).where(Job.id == job_id)
                result = await db.execute(stmt)
                job = result.scalar_one_or_none()
                
                if job:
                    job.status = JobStatus.FAILED
                    job.completed_at = datetime.now(timezone.utc)
                    job.metadata = job.metadata or {}
                    job.metadata["ingestion_failed"] = "Max retries exceeded"
                    await db.commit()
        except Exception as e:
            logger.error("Failed to update job status", error=str(e))