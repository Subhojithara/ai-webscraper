"""
Upload router for handling file uploads and S3 operations.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from scraper_lib import get_database_session
from scraper_lib.observability import monitor_function
from ..models import UploadUrlRequest, UploadUrlResponse
from ..services.upload_service import UploadService

router = APIRouter()
logger = structlog.get_logger("api-server.upload")


@router.post("/url", response_model=UploadUrlResponse, status_code=status.HTTP_200_OK)
@monitor_function("get_upload_url")
async def get_upload_url(
    upload_request: UploadUrlRequest,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Get a pre-signed URL for file upload to S3.
    
    This endpoint generates a secure, time-limited URL that clients can use
    to upload files directly to S3 without going through the API server.
    """
    logger.info("Generating upload URL", filename=upload_request.filename)
    
    try:
        upload_service = UploadService()
        upload_data = await upload_service.generate_upload_url(upload_request)
        
        logger.info(
            "Upload URL generated", 
            filename=upload_request.filename,
            file_path=upload_data.file_path
        )
        
        return upload_data
        
    except ValueError as e:
        logger.error("Invalid upload request", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to generate upload URL", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to generate upload URL")


@router.post("/verify")
@monitor_function("verify_upload")
async def verify_upload(
    file_path: str,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Verify that a file has been successfully uploaded to S3.
    
    This endpoint can be called after file upload to confirm
    that the file is accessible and ready for processing.
    """
    logger.info("Verifying file upload", file_path=file_path)
    
    try:
        upload_service = UploadService()
        is_valid = await upload_service.verify_upload(file_path)
        
        if not is_valid:
            raise HTTPException(status_code=404, detail="File not found or not accessible")
        
        logger.info("File upload verified", file_path=file_path)
        return {"status": "verified", "file_path": file_path}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to verify upload", file_path=file_path, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to verify upload")