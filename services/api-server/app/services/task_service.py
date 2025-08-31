"""
Task service for managing scraping tasks.
"""

import uuid
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
import structlog

from scraper_lib.database import Task, TaskStatus
from ..models import TaskListResponse, TaskResponse, PaginationParams, BulkActionResponse

logger = structlog.get_logger("api-server.task_service")


class TaskService:
    """Service for managing scraping tasks."""
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
    
    async def list_job_tasks(
        self, 
        job_id: uuid.UUID,
        pagination: PaginationParams,
        status_filter: Optional[str] = None
    ) -> TaskListResponse:
        """List tasks for a specific job."""
        # Build query
        stmt = select(Task).where(Task.job_id == job_id)
        
        # Apply status filter
        if status_filter:
            try:
                status_enum = TaskStatus(status_filter)
                stmt = stmt.where(Task.status == status_enum)
            except ValueError:
                pass  # Invalid status, ignore filter
        
        # Get total count
        from sqlalchemy import func
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_result = await self.db.execute(count_stmt)
        total = total_result.scalar()
        
        # Apply pagination
        offset = (pagination.page - 1) * pagination.page_size
        stmt = stmt.offset(offset).limit(pagination.page_size)
        stmt = stmt.order_by(Task.created_at.desc())
        
        # Execute query
        result = await self.db.execute(stmt)
        tasks = result.scalars().all()
        
        # Convert to response models
        task_responses = [TaskResponse.model_validate(task) for task in tasks]
        
        return TaskListResponse(
            tasks=task_responses,
            total=total,
            page=pagination.page,
            page_size=pagination.page_size,
            has_next=(pagination.page * pagination.page_size) < total,
            has_previous=pagination.page > 1
        )
    
    async def retry_tasks(
        self, 
        task_ids: List[uuid.UUID],
        reset_retry_count: bool = False
    ) -> BulkActionResponse:
        """Retry failed tasks."""
        success_count = 0
        failure_count = 0
        errors = []
        
        for task_id in task_ids:
            try:
                stmt = select(Task).where(Task.id == task_id)
                result = await self.db.execute(stmt)
                task = result.scalar_one_or_none()
                
                if not task:
                    errors.append(f"Task {task_id} not found")
                    failure_count += 1
                    continue
                
                if task.status != TaskStatus.FAILED:
                    errors.append(f"Task {task_id} is not in failed status")
                    failure_count += 1
                    continue
                
                # Reset task for retry
                task.status = TaskStatus.PENDING
                task.error_message = None
                task.started_at = None
                task.completed_at = None
                
                if reset_retry_count:
                    task.retry_count = 0
                
                success_count += 1
                
            except Exception as e:
                logger.error("Failed to retry task", task_id=str(task_id), error=str(e))
                errors.append(f"Error retrying task {task_id}: {str(e)}")
                failure_count += 1
        
        if success_count > 0:
            await self.db.flush()
        
        return BulkActionResponse(
            success_count=success_count,
            failure_count=failure_count,
            errors=errors
        )