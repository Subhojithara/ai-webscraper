"""
Proxy manager for rotating and managing proxy servers.
"""

import asyncio
import random
import time
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse
import httpx
import structlog

from scraper_lib import get_settings, get_redis_client
from scraper_lib.observability import monitor_function

logger = structlog.get_logger("http-scraper-worker.proxy_manager")


class ProxyManager:
    """Manages proxy rotation and health checking."""
    
    def __init__(self):
        self.settings = get_settings()
        self.redis_client = get_redis_client()
        self.proxies = []
        self.proxy_stats = {}
        self.health_check_interval = 300  # 5 minutes
        self.last_health_check = 0
        self._lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize proxy manager with configured proxies."""
        if not self.settings.proxy_enabled:
            logger.info("Proxy rotation disabled")
            return
        
        if not self.settings.proxy_urls:
            logger.warning("Proxy enabled but no proxy URLs configured")
            return
        
        logger.info("Initializing proxy manager", proxy_count=len(self.settings.proxy_urls))
        
        # Parse and validate proxy URLs
        for proxy_url in self.settings.proxy_urls:
            try:
                parsed = urlparse(proxy_url)
                if parsed.scheme and parsed.netloc:
                    self.proxies.append({
                        "url": proxy_url,
                        "scheme": parsed.scheme,
                        "host": parsed.hostname,
                        "port": parsed.port,
                        "status": "unknown",
                        "last_check": 0,
                        "success_count": 0,
                        "failure_count": 0,
                        "avg_response_time": 0.0
                    })
                else:
                    logger.warning("Invalid proxy URL format", proxy_url=proxy_url)
            except Exception as e:
                logger.error("Failed to parse proxy URL", proxy_url=proxy_url, error=str(e))
        
        if self.proxies:
            # Perform initial health check
            await self.health_check_all_proxies()
            
            # Start background health checking
            asyncio.create_task(self.background_health_check())
            
            logger.info("Proxy manager initialized", active_proxies=len(self.get_healthy_proxies()))
        else:
            logger.warning("No valid proxies configured")
    
    @monitor_function("get_proxy")
    async def get_proxy(self) -> Optional[str]:
        """Get a healthy proxy for making requests."""
        if not self.settings.proxy_enabled or not self.proxies:
            return None
        
        async with self._lock:
            healthy_proxies = self.get_healthy_proxies()
            
            if not healthy_proxies:
                # If no healthy proxies, trigger health check and try again
                logger.warning("No healthy proxies available, triggering health check")
                await self.health_check_all_proxies()
                healthy_proxies = self.get_healthy_proxies()
                
                if not healthy_proxies:
                    logger.error("Still no healthy proxies after health check")
                    return None
            
            # Select proxy based on performance
            if self.settings.proxy_rotation_enabled:
                proxy = self.select_best_proxy(healthy_proxies)
            else:
                proxy = random.choice(healthy_proxies)
            
            logger.debug("Selected proxy", proxy_url=proxy["url"])
            return proxy["url"]
    
    def get_healthy_proxies(self) -> List[Dict[str, Any]]:
        """Get list of healthy proxies."""
        return [proxy for proxy in self.proxies if proxy["status"] == "healthy"]
    
    def select_best_proxy(self, healthy_proxies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select the best proxy based on performance metrics."""
        if len(healthy_proxies) == 1:
            return healthy_proxies[0]
        
        # Calculate score for each proxy based on success rate and response time
        scored_proxies = []
        
        for proxy in healthy_proxies:
            total_requests = proxy["success_count"] + proxy["failure_count"]
            if total_requests > 0:
                success_rate = proxy["success_count"] / total_requests
                # Lower response time is better, so invert it
                response_time_score = 1.0 / (proxy["avg_response_time"] + 0.1)
                score = (success_rate * 0.7) + (response_time_score * 0.3)
            else:
                score = 0.5  # Default score for new proxies
            
            scored_proxies.append((proxy, score))
        
        # Sort by score (highest first) and add some randomness
        scored_proxies.sort(key=lambda x: x[1], reverse=True)
        
        # Select from top 3 to add some randomness
        top_proxies = scored_proxies[:min(3, len(scored_proxies))]
        selected_proxy, _ = random.choice(top_proxies)
        
        return selected_proxy
    
    async def report_proxy_result(self, proxy_url: str, success: bool, response_time: float):
        """Report the result of using a proxy."""
        async with self._lock:
            for proxy in self.proxies:
                if proxy["url"] == proxy_url:
                    if success:
                        proxy["success_count"] += 1
                        proxy["status"] = "healthy"
                    else:
                        proxy["failure_count"] += 1
                        
                        # Mark as unhealthy if failure rate is too high
                        total_requests = proxy["success_count"] + proxy["failure_count"]
                        if total_requests >= 10:  # Only after some requests
                            failure_rate = proxy["failure_count"] / total_requests
                            if failure_rate > 0.5:  # More than 50% failure rate
                                proxy["status"] = "unhealthy"
                    
                    # Update average response time
                    if proxy["avg_response_time"] == 0:
                        proxy["avg_response_time"] = response_time
                    else:
                        # Exponential moving average
                        proxy["avg_response_time"] = (
                            proxy["avg_response_time"] * 0.8 + response_time * 0.2
                        )
                    
                    break
    
    async def health_check_all_proxies(self):
        """Perform health check on all proxies."""
        if not self.proxies:
            return
        
        logger.info("Starting proxy health check", proxy_count=len(self.proxies))
        
        # Test URL for health checks
        test_urls = [
            "http://httpbin.org/ip",
            "https://httpbin.org/get",
            "http://www.google.com"
        ]
        
        # Check all proxies concurrently
        tasks = []
        for proxy in self.proxies:
            task = asyncio.create_task(
                self.health_check_proxy(proxy, test_urls[0])
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        healthy_count = len(self.get_healthy_proxies())
        logger.info("Proxy health check completed", healthy_proxies=healthy_count)
        
        self.last_health_check = time.time()
    
    async def health_check_proxy(self, proxy: Dict[str, Any], test_url: str):
        """Health check a single proxy."""
        start_time = time.time()
        
        try:
            timeout = httpx.Timeout(connect=10.0, read=10.0)
            
            async with httpx.AsyncClient(
                proxies={"http://": proxy["url"], "https://": proxy["url"]},
                timeout=timeout,
                verify=False
            ) as client:
                response = await client.get(test_url)
                
                if response.status_code == 200:
                    response_time = time.time() - start_time
                    proxy["status"] = "healthy"
                    proxy["last_check"] = time.time()
                    proxy["avg_response_time"] = response_time
                    
                    logger.debug(
                        "Proxy health check passed",
                        proxy_url=proxy["url"],
                        response_time=round(response_time, 3)
                    )
                else:
                    proxy["status"] = "unhealthy"
                    logger.warning(
                        "Proxy health check failed - bad status",
                        proxy_url=proxy["url"],
                        status_code=response.status_code
                    )
                    
        except Exception as e:
            proxy["status"] = "unhealthy"
            proxy["last_check"] = time.time()
            
            logger.warning(
                "Proxy health check failed",
                proxy_url=proxy["url"],
                error=str(e)
            )
    
    async def background_health_check(self):
        """Background task for periodic proxy health checking."""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                current_time = time.time()
                if current_time - self.last_health_check >= self.health_check_interval:
                    await self.health_check_all_proxies()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in background health check", error=str(e))
                await asyncio.sleep(60)  # Wait before retrying
    
    async def get_proxy_stats(self) -> Dict[str, Any]:
        """Get proxy statistics."""
        if not self.proxies:
            return {"total_proxies": 0, "healthy_proxies": 0, "proxies": []}
        
        stats = {
            "total_proxies": len(self.proxies),
            "healthy_proxies": len(self.get_healthy_proxies()),
            "last_health_check": self.last_health_check,
            "proxies": []
        }
        
        for proxy in self.proxies:
            total_requests = proxy["success_count"] + proxy["failure_count"]
            success_rate = (
                proxy["success_count"] / total_requests 
                if total_requests > 0 else 0
            )
            
            proxy_stats = {
                "url": proxy["url"],
                "status": proxy["status"],
                "success_count": proxy["success_count"],
                "failure_count": proxy["failure_count"],
                "success_rate": round(success_rate, 3),
                "avg_response_time": round(proxy["avg_response_time"], 3),
                "last_check": proxy["last_check"]
            }
            
            stats["proxies"].append(proxy_stats)
        
        return stats
    
    async def cache_proxy_stats(self):
        """Cache proxy statistics in Redis."""
        try:
            stats = await self.get_proxy_stats()
            await self.redis_client.set(
                "proxy_stats",
                stats,
                ttl=300  # 5 minutes
            )
        except Exception as e:
            logger.error("Failed to cache proxy stats", error=str(e))