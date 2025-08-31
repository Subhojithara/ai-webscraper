"""
Authentication middleware for API server.
"""

from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
import structlog

logger = structlog.get_logger("api-server.auth")


class AuthMiddleware(BaseHTTPMiddleware):
    """Authentication middleware for API requests."""
    
    def __init__(self, app):
        super().__init__(app)
        self.security = HTTPBearer(auto_error=False)
        
        # Public endpoints that don't require authentication
        self.public_endpoints = {
            "/health",
            "/health/ready", 
            "/health/live",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/metrics"
        }
    
    async def dispatch(self, request: Request, call_next):
        """Process authentication for incoming requests."""
        # Skip auth for public endpoints
        if any(request.url.path.startswith(endpoint) for endpoint in self.public_endpoints):
            return await call_next(request)
        
        # For now, we'll implement a simple token-based auth
        # In production, this would integrate with your auth system
        auth_header = request.headers.get("Authorization")
        
        if not auth_header:
            logger.warning("Missing authorization header", path=request.url.path)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization header required",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        try:
            # Validate token (simplified for demo)
            if not auth_header.startswith("Bearer "):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authorization header format"
                )
            
            token = auth_header.split(" ")[1]
            
            # In production, validate JWT token here
            # For now, accept any non-empty token
            if not token:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token"
                )
            
            # Add user info to request state
            request.state.user_id = "demo_user"  # Would come from token
            request.state.token = token
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Authentication error", error=str(e))
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication failed"
            )
        
        return await call_next(request)