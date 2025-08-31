"""
Job Coordinator Service for orchestration, state management, and progress tracking.
Manages the lifecycle of scraping jobs and coordinates between different workers.
"""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from enum import Enum

import structlog
from sqlalchemy import select, update, func
from sqlalchemy.orm import selectinload

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from scraper_lib import get_settings, get_database_session, get_redis_client
from scraper_lib.database import Job, Task, Result, JobStatus, TaskStatus, WorkerType
from scraper_lib.observability import get_metrics_collector

logger = structlog.get_logger(__name__)


class CoordinatorAction(Enum):
    """Actions that the coordinator can take."""
    START_JOB = "start_job"
    PAUSE_JOB = "pause_job"
    RESUME_JOB = "resume_job"
    CANCEL_JOB = "cancel_job"
    RETRY_FAILED_TASKS = "retry_failed_tasks"
    SCALE_WORKERS = "scale_workers"
    CLEANUP_COMPLETED_JOBS = "cleanup_completed_jobs"


@dataclass
class JobProgress:
    """Container for job progress information."""
    job_id: str
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    in_progress_tasks: int
    pending_tasks: int
    progress_percentage: float
    estimated_completion: Optional[datetime]
    average_task_duration: float
    current_throughput: float


@dataclass
class WorkerStats:
    """Container for worker statistics."""
    worker_type: str
    active_workers: int
    queue_size: int
    processing_rate: float
    error_rate: float
    avg_processing_time: float


class JobCoordinator:
    """
    Advanced job coordinator for managing scraping job lifecycle and orchestration.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.metrics = get_metrics_collector("job-coordinator")
        self.redis_client = get_redis_client()
        
        # Coordinator configuration
        self.check_interval = 30  # seconds
        self.cleanup_interval = 3600  # 1 hour
        self.max_retry_attempts = 3
        self.job_timeout = timedelta(hours=24)
        
        # Worker management
        self.worker_types = [WorkerType.HTTP, WorkerType.HEADLESS, WorkerType.PROCESSING, WorkerType.CRAWL4AI]
        self.queue_names = {
            WorkerType.HTTP: "scrape_http",
            WorkerType.HEADLESS: "scrape_headless", 
            WorkerType.PROCESSING: "needs_cleaning",
            WorkerType.CRAWL4AI: "scrape_crawl4ai"
        }
        
        # State tracking
        self.active_jobs: Set[str] = set()
        self.job_stats: Dict[str, JobProgress] = {}
        self.worker_stats: Dict[str, WorkerStats] = {}
        
        self.running = False
    
    async def start(self):
        """Start the job coordinator."""
        try:
            self.running = True
            logger.info("Job Coordinator started")
            
            # Start background tasks
            tasks = [
                asyncio.create_task(self._monitor_jobs()),
                asyncio.create_task(self._manage_worker_queues()),
                asyncio.create_task(self._cleanup_completed_jobs()),
                asyncio.create_task(self._collect_metrics()),
                asyncio.create_task(self._handle_job_actions())
            ]
            
            # Wait for all tasks to complete
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Job Coordinator failed: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the job coordinator."""
        self.running = False
        logger.info("Job Coordinator stopped")
    
    async def _monitor_jobs(self):
        """Monitor active jobs and update their status."""
        while self.running:
            try:
                await self._update_job_statuses()
                await self._detect_stalled_jobs()
                await self._handle_failed_jobs()
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in job monitoring: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _update_job_statuses(self):
        """Update status of all active jobs."""
        async with get_database_session() as session:
            # Get all active jobs
            result = await session.execute(
                select(Job).where(
                    Job.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
                ).options(selectinload(Job.tasks))
            )
            jobs = result.scalars().all()
            
            for job in jobs:
                # Calculate job progress
                progress = await self._calculate_job_progress(job)
                self.job_stats[str(job.id)] = progress
                
                # Update job status based on progress
                new_status = await self._determine_job_status(job, progress)
                
                if job.status != new_status:
                    job.status = new_status
                    job.updated_at = datetime.now(timezone.utc)
                    
                    if new_status == JobStatus.COMPLETED:
                        job.completed_at = datetime.now(timezone.utc)
                    
                    logger.info(f"Job {job.id} status updated to {new_status}")
                
                # Update progress metrics
                job.completed_urls = progress.completed_tasks
                job.failed_urls = progress.failed_tasks
                
            await session.commit()
    
    async def _calculate_job_progress(self, job: Job) -> JobProgress:
        """Calculate detailed progress information for a job."""
        tasks = job.tasks
        total_tasks = len(tasks)
        
        if total_tasks == 0:
            return JobProgress(
                job_id=str(job.id),
                total_tasks=0,
                completed_tasks=0,
                failed_tasks=0,
                in_progress_tasks=0,
                pending_tasks=0,
                progress_percentage=0.0,
                estimated_completion=None,
                average_task_duration=0.0,
                current_throughput=0.0
            )
        
        # Count tasks by status
        completed_tasks = sum(1 for task in tasks if task.status == TaskStatus.COMPLETED)
        failed_tasks = sum(1 for task in tasks if task.status == TaskStatus.FAILED)
        in_progress_tasks = sum(1 for task in tasks if task.status == TaskStatus.IN_PROGRESS)
        pending_tasks = sum(1 for task in tasks if task.status == TaskStatus.PENDING)
        
        # Calculate progress percentage
        progress_percentage = (completed_tasks / total_tasks) * 100
        
        # Calculate average task duration
        completed_task_durations = [
            task.duration_seconds for task in tasks 
            if task.status == TaskStatus.COMPLETED and task.duration_seconds
        ]
        avg_duration = sum(completed_task_durations) / len(completed_task_durations) if completed_task_durations else 0
        
        # Calculate current throughput (tasks per minute)
        recent_completions = [
            task for task in tasks 
            if task.status == TaskStatus.COMPLETED and task.completed_at and
            task.completed_at > datetime.now(timezone.utc) - timedelta(minutes=10)
        ]
        current_throughput = len(recent_completions) / 10  # tasks per minute
        
        # Estimate completion time
        remaining_tasks = pending_tasks + in_progress_tasks
        estimated_completion = None
        if current_throughput > 0 and remaining_tasks > 0:
            minutes_remaining = remaining_tasks / current_throughput
            estimated_completion = datetime.now(timezone.utc) + timedelta(minutes=minutes_remaining)
        
        return JobProgress(
            job_id=str(job.id),
            total_tasks=total_tasks,
            completed_tasks=completed_tasks,
            failed_tasks=failed_tasks,
            in_progress_tasks=in_progress_tasks,
            pending_tasks=pending_tasks,
            progress_percentage=progress_percentage,
            estimated_completion=estimated_completion,
            average_task_duration=avg_duration,
            current_throughput=current_throughput
        )
    
    async def _determine_job_status(self, job: Job, progress: JobProgress) -> JobStatus:
        """Determine the appropriate status for a job based on its progress."""
        # Job is completed if all tasks are done (completed or failed)
        if progress.pending_tasks == 0 and progress.in_progress_tasks == 0:
            if progress.completed_tasks > 0:
                return JobStatus.COMPLETED
            else:
                return JobStatus.FAILED
        
        # Job is processing if any tasks are in progress or completed
        if progress.in_progress_tasks > 0 or progress.completed_tasks > 0:
            return JobStatus.PROCESSING
        
        # Otherwise, job is still pending
        return JobStatus.PENDING
    
    async def _detect_stalled_jobs(self):
        """Detect and handle stalled jobs."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
        
        async with get_database_session() as session:
            # Find jobs with tasks that have been in progress for too long
            result = await session.execute(
                select(Task).where(
                    Task.status == TaskStatus.IN_PROGRESS,
                    Task.started_at < cutoff_time
                )
            )
            stalled_tasks = result.scalars().all()
            
            for task in stalled_tasks:
                logger.warning(f"Detected stalled task {task.id}, resetting to pending")
                task.status = TaskStatus.PENDING
                task.started_at = None
                task.retry_count += 1
                
                # If max retries exceeded, mark as failed
                if task.retry_count >= task.max_retries:
                    task.status = TaskStatus.FAILED
                    task.error_message = "Task stalled - exceeded maximum retries"
            
            await session.commit()
    
    async def _handle_failed_jobs(self):
        """Handle jobs with too many failed tasks."""
        async with get_database_session() as session:
            # Get jobs with high failure rates
            result = await session.execute(
                select(Job).where(
                    Job.status == JobStatus.PROCESSING
                ).options(selectinload(Job.tasks))
            )
            jobs = result.scalars().all()
            
            for job in jobs:
                if not job.tasks:
                    continue
                
                total_tasks = len(job.tasks)
                failed_tasks = sum(1 for task in job.tasks if task.status == TaskStatus.FAILED)
                failure_rate = failed_tasks / total_tasks
                
                # If failure rate is too high, consider job failed
                if failure_rate > 0.8 and total_tasks > 10:
                    logger.warning(f"Job {job.id} has high failure rate ({failure_rate:.2%}), marking as failed")
                    job.status = JobStatus.FAILED
                    job.completed_at = datetime.now(timezone.utc)
                    
                    # Record metric
                    self.metrics.jobs_failed_total.labels(reason="high_failure_rate").inc()
            
            await session.commit()
    
    async def _manage_worker_queues(self):
        """Manage worker queues and load balancing."""
        while self.running:
            try:
                await self._balance_task_distribution()
                await self._monitor_queue_health()
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in worker queue management: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _balance_task_distribution(self):
        """Balance task distribution across different worker types."""
        async with get_database_session() as session:
            # Get pending tasks
            result = await session.execute(
                select(Task).where(Task.status == TaskStatus.PENDING)
            )
            pending_tasks = result.scalars().all()
            
            # Group tasks by worker type preference
            task_groups = {worker_type: [] for worker_type in self.worker_types}
            
            for task in pending_tasks:
                # Determine optimal worker type based on task characteristics
                optimal_worker = await self._determine_optimal_worker(task)
                task_groups[optimal_worker].append(task)
            
            # Distribute tasks to queues
            for worker_type, tasks in task_groups.items():
                if tasks:
                    await self._enqueue_tasks(worker_type, tasks)
    
    async def _determine_optimal_worker(self, task: Task) -> WorkerType:
        """Determine the optimal worker type for a given task."""
        url = task.url
        task_config = task.config or {}
        
        # Check if AI extraction is explicitly requested
        if (
            task_config.get("enable_ai_extraction", False) or
            task_config.get("extraction_strategy") == "llm" or
            task_config.get("extraction_strategy") == "cosine" or
            task_config.get("semantic_filter") is not None or
            task_config.get("complex_content", False)
        ):
            return WorkerType.CRAWL4AI
        
        # Check for content that benefits from AI enhancement
        ai_beneficial_domains = [
            'news', 'blog', 'article', 'post', 'content', 'medium.com',
            'substack.com', 'linkedin.com/pulse', 'wordpress.com'
        ]
        
        if any(domain in url.lower() for domain in ai_beneficial_domains):
            # Use Crawl4AI for better content extraction on these domains
            if task_config.get("enable_smart_routing", True):
                return WorkerType.CRAWL4AI
        
        # Check for JavaScript-heavy sites that need headless browser
        js_indicators = ['spa', 'react', 'angular', 'vue', 'app', 'javascript:']
        if any(indicator in url.lower() for indicator in js_indicators):
            return WorkerType.HEADLESS
        
        # Default to HTTP worker for simple sites
        return WorkerType.HTTP
    
    async def _enqueue_tasks(self, worker_type: WorkerType, tasks: List[Task]):
        """Enqueue tasks to the appropriate worker queue."""
        queue_name = self.queue_names.get(worker_type)
        if not queue_name:
            return
        
        for task in tasks:
            message = {
                "task_id": str(task.id),
                "job_id": str(task.job_id),
                "url": task.url,
                "config": task.config or {},
                "priority": task.priority
            }
            
            await self.redis_client.xadd(queue_name, message)
            
            # Update task status
            task.status = TaskStatus.PENDING
        
        logger.info(f"Enqueued {len(tasks)} tasks to {queue_name}")
    
    async def _monitor_queue_health(self):
        """Monitor the health of worker queues."""
        for worker_type, queue_name in self.queue_names.items():
            try:
                # Get queue information
                info = await self.redis_client.xinfo_stream(queue_name)
                queue_length = info.get('length', 0)
                
                # Get consumer group information
                try:
                    groups = await self.redis_client.xinfo_groups(queue_name)
                    consumers_count = sum(group.get('consumers', 0) for group in groups)
                except:
                    consumers_count = 0
                
                # Update worker stats
                self.worker_stats[worker_type.value] = WorkerStats(
                    worker_type=worker_type.value,
                    active_workers=consumers_count,
                    queue_size=queue_length,
                    processing_rate=0.0,  # Would be calculated from historical data
                    error_rate=0.0,       # Would be calculated from metrics
                    avg_processing_time=0.0  # Would be calculated from task durations
                )
                
                # Alert if queue is too long
                if queue_length > 1000:
                    logger.warning(f"Queue {queue_name} is backing up: {queue_length} messages")
                    self.metrics.queue_backlog_alert.labels(queue=queue_name).inc()
                
            except Exception as e:
                logger.error(f"Error monitoring queue {queue_name}: {e}")
    
    async def _cleanup_completed_jobs(self):
        """Clean up old completed jobs and their data."""
        while self.running:
            try:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
                
                async with get_database_session() as session:
                    # Find old completed jobs
                    result = await session.execute(
                        select(Job).where(
                            Job.status.in_([JobStatus.COMPLETED, JobStatus.FAILED]),
                            Job.completed_at < cutoff_date
                        )
                    )
                    old_jobs = result.scalars().all()
                    
                    for job in old_jobs:
                        logger.info(f"Cleaning up old job {job.id}")
                        
                        # Archive job data (implementation would move to archive storage)
                        await self._archive_job_data(job)
                        
                        # Delete job from database (cascade will handle tasks and results)
                        await session.delete(job)
                        
                        # Record metric
                        self.metrics.jobs_cleaned_up_total.inc()
                    
                    await session.commit()
                
                await asyncio.sleep(self.cleanup_interval)
                
            except Exception as e:
                logger.error(f"Error in job cleanup: {e}")
                await asyncio.sleep(self.cleanup_interval)
    
    async def _archive_job_data(self, job: Job):
        """Archive job data to long-term storage."""
        # Implementation would move job data to archive storage (S3, etc.)
        logger.info(f"Archiving job {job.id} data")
    
    async def _collect_metrics(self):
        """Collect and expose metrics about job coordination."""
        while self.running:
            try:
                # Update job metrics
                for job_id, progress in self.job_stats.items():
                    self.metrics.job_progress.labels(job_id=job_id).set(progress.progress_percentage)
                    self.metrics.job_tasks_total.labels(job_id=job_id).set(progress.total_tasks)
                    self.metrics.job_tasks_completed.labels(job_id=job_id).set(progress.completed_tasks)
                    self.metrics.job_tasks_failed.labels(job_id=job_id).set(progress.failed_tasks)
                
                # Update worker metrics
                for worker_type, stats in self.worker_stats.items():
                    self.metrics.worker_queue_size.labels(worker_type=worker_type).set(stats.queue_size)
                    self.metrics.active_workers.labels(worker_type=worker_type).set(stats.active_workers)
                
                await asyncio.sleep(60)  # Collect metrics every minute
                
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(60)
    
    async def _handle_job_actions(self):
        """Handle job actions from API or other services."""
        while self.running:
            try:
                # Listen for job actions on Redis
                action_queue = "job_actions"
                
                # This would listen for actions like pause, resume, cancel, etc.
                # Implementation would process action messages
                
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error handling job actions: {e}")
                await asyncio.sleep(5)
    
    async def pause_job(self, job_id: str) -> bool:
        """Pause a running job."""
        try:
            async with get_database_session() as session:
                result = await session.execute(
                    select(Job).where(Job.id == job_id)
                )
                job = result.scalar_one_or_none()
                
                if not job or job.status != JobStatus.PROCESSING:
                    return False
                
                job.status = JobStatus.PAUSED
                job.updated_at = datetime.now(timezone.utc)
                
                await session.commit()
                
                logger.info(f"Job {job_id} paused")
                return True
                
        except Exception as e:
            logger.error(f"Error pausing job {job_id}: {e}")
            return False
    
    async def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        try:
            async with get_database_session() as session:
                result = await session.execute(
                    select(Job).where(Job.id == job_id)
                )
                job = result.scalar_one_or_none()
                
                if not job or job.status != JobStatus.PAUSED:
                    return False
                
                job.status = JobStatus.PROCESSING
                job.updated_at = datetime.now(timezone.utc)
                
                await session.commit()
                
                logger.info(f"Job {job_id} resumed")
                return True
                
        except Exception as e:
            logger.error(f"Error resuming job {job_id}: {e}")
            return False
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job and mark all pending tasks as cancelled."""
        try:
            async with get_database_session() as session:
                result = await session.execute(
                    select(Job).where(Job.id == job_id).options(selectinload(Job.tasks))
                )
                job = result.scalar_one_or_none()
                
                if not job:
                    return False
                
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.now(timezone.utc)
                job.updated_at = datetime.now(timezone.utc)
                
                # Cancel all pending and in-progress tasks
                for task in job.tasks:
                    if task.status in [TaskStatus.PENDING, TaskStatus.IN_PROGRESS]:
                        task.status = TaskStatus.CANCELLED
                
                await session.commit()
                
                logger.info(f"Job {job_id} cancelled")
                return True
                
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            return False
    
    async def get_job_progress(self, job_id: str) -> Optional[JobProgress]:
        """Get current progress for a job."""
        return self.job_stats.get(job_id)
    
    async def get_system_stats(self) -> Dict[str, Any]:
        """Get overall system statistics."""
        async with get_database_session() as session:
            # Get job counts by status
            result = await session.execute(
                select(Job.status, func.count()).group_by(Job.status)
            )
            job_counts = dict(result.fetchall())
            
            # Get task counts by status
            result = await session.execute(
                select(Task.status, func.count()).group_by(Task.status)
            )
            task_counts = dict(result.fetchall())
        
        return {
            'job_counts': job_counts,
            'task_counts': task_counts,
            'worker_stats': self.worker_stats,
            'active_jobs': len(self.active_jobs),
            'coordinator_uptime': time.time()  # Would track actual uptime
        }


class JobCoordinatorService:
    """Main service class for job coordination."""
    
    def __init__(self):
        self.coordinator = JobCoordinator()
    
    async def start(self):
        """Start the job coordinator service."""
        await self.coordinator.start()
    
    async def stop(self):
        """Stop the job coordinator service."""
        await self.coordinator.stop()


# Main execution
async def main():
    """Main entry point for the job coordinator."""
    service = JobCoordinatorService()
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())