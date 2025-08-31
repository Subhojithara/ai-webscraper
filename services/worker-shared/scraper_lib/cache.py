"""
Redis client and queue management for K-Scrape Nexus.
Provides async Redis operations with connection pooling and queue abstractions.
"""

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from .config import get_settings


@dataclass
class QueueMessage:
    """Represents a message in a Redis queue."""
    id: str
    data: Dict[str, Any]
    timestamp: datetime
    retry_count: int = 0
    max_retries: int = 3
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "id": self.id,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "retry_count": self.retry_count,
            "max_retries": self.max_retries
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueueMessage":
        """Create message from dictionary."""
        return cls(
            id=data["id"],
            data=data["data"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3)
        )


class RedisClient:
    """Async Redis client with connection pooling and high-level operations."""
    
    def __init__(self, redis_url: str, max_connections: int = 50):
        self.redis_url = redis_url
        self.max_connections = max_connections
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Initialize Redis connection pool."""
        if self._pool is None:
            self._pool = ConnectionPool.from_url(
                self.redis_url,
                max_connections=self.max_connections,
                retry_on_timeout=True,
                health_check_interval=30
            )
            self._client = redis.Redis(connection_pool=self._pool)
    
    async def disconnect(self):
        """Close Redis connections."""
        if self._client:
            await self._client.close()
        if self._pool:
            await self._pool.disconnect()
    
    async def ping(self) -> bool:
        """Test Redis connection."""
        try:
            if self._client:
                await self._client.ping()
                return True
        except Exception:
            pass
        return False
    
    async def set(
        self, 
        key: str, 
        value: Union[str, Dict, List], 
        ttl: Optional[int] = None
    ) -> bool:
        """Set key-value pair with optional TTL."""
        if not self._client:
            await self.connect()
        
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        
        result = await self._client.set(key, value, ex=ttl)
        return bool(result)
    
    async def get(self, key: str) -> Optional[Union[str, Dict, List]]:
        """Get value by key, automatically parsing JSON."""
        if not self._client:
            await self.connect()
        
        value = await self._client.get(key)
        if value is None:
            return None
        
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value.decode() if isinstance(value, bytes) else value
    
    async def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        if not self._client:
            await self.connect()
        return await self._client.delete(*keys)
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self._client:
            await self.connect()
        return bool(await self._client.exists(key))
    
    async def expire(self, key: str, ttl: int) -> bool:
        """Set TTL for existing key."""
        if not self._client:
            await self.connect()
        return bool(await self._client.expire(key, ttl))
    
    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment key value."""
        if not self._client:
            await self.connect()
        return await self._client.incr(key, amount)
    
    async def decr(self, key: str, amount: int = 1) -> int:
        """Decrement key value."""
        if not self._client:
            await self.connect()
        return await self._client.decr(key, amount)
    
    async def hset(self, name: str, mapping: Dict[str, Any]) -> int:
        """Set hash fields."""
        if not self._client:
            await self.connect()
        
        # Convert values to JSON strings if needed
        json_mapping = {}
        for key, value in mapping.items():
            if isinstance(value, (dict, list)):
                json_mapping[key] = json.dumps(value)
            else:
                json_mapping[key] = str(value)
        
        return await self._client.hset(name, mapping=json_mapping)
    
    async def hget(self, name: str, key: str) -> Optional[Any]:
        """Get hash field value."""
        if not self._client:
            await self.connect()
        
        value = await self._client.hget(name, key)
        if value is None:
            return None
        
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value.decode() if isinstance(value, bytes) else value
    
    async def hgetall(self, name: str) -> Dict[str, Any]:
        """Get all hash fields."""
        if not self._client:
            await self.connect()
        
        result = await self._client.hgetall(name)
        parsed_result = {}
        
        for key, value in result.items():
            key_str = key.decode() if isinstance(key, bytes) else key
            try:
                parsed_result[key_str] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                parsed_result[key_str] = value.decode() if isinstance(value, bytes) else value
        
        return parsed_result


class RedisQueue:
    """High-level Redis queue operations using Redis Streams."""
    
    def __init__(self, redis_client: RedisClient, queue_name: str):
        self.redis_client = redis_client
        self.queue_name = queue_name
        self.consumer_group = f"{queue_name}_consumers"
    
    async def setup_consumer_group(self, consumer_name: str = "default"):
        """Setup consumer group for the queue."""
        try:
            await self.redis_client._client.xgroup_create(
                self.queue_name, 
                self.consumer_group, 
                id="0", 
                mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
    
    async def publish(
        self, 
        data: Dict[str, Any], 
        message_id: Optional[str] = None,
        priority: int = 1
    ) -> str:
        """Publish message to queue."""
        if not self.redis_client._client:
            await self.redis_client.connect()
        
        if message_id is None:
            message_id = str(uuid.uuid4())
        
        message = QueueMessage(
            id=message_id,
            data=data,
            timestamp=datetime.utcnow(),
            retry_count=0
        )
        
        # Add priority to message for sorting
        message_data = message.to_dict()
        message_data["priority"] = priority
        
        # Serialize message data
        serialized_data = {}
        for key, value in message_data.items():
            if isinstance(value, (dict, list)):
                serialized_data[key] = json.dumps(value)
            else:
                serialized_data[key] = str(value)
        
        # Use timestamp for message ID to maintain order
        stream_id = await self.redis_client._client.xadd(
            self.queue_name,
            serialized_data,
            id="*",
            maxlen=100000  # Limit stream length
        )
        
        return stream_id
    
    async def consume(
        self, 
        consumer_name: str = "default",
        timeout: int = 5000,
        count: int = 1
    ) -> List[QueueMessage]:
        """Consume messages from queue."""
        if not self.redis_client._client:
            await self.redis_client.connect()
        
        # Setup consumer group if needed
        await self.setup_consumer_group(consumer_name)
        
        try:
            # Read new messages
            messages = await self.redis_client._client.xreadgroup(
                self.consumer_group,
                consumer_name,
                {self.queue_name: ">"},
                count=count,
                block=timeout
            )
            
            result = []
            if messages:
                for stream_name, stream_messages in messages:
                    for message_id, fields in stream_messages:
                        # Deserialize message data
                        message_data = {}
                        for key, value in fields.items():
                            key_str = key.decode() if isinstance(key, bytes) else key
                            value_str = value.decode() if isinstance(value, bytes) else value
                            
                            try:
                                message_data[key_str] = json.loads(value_str)
                            except (json.JSONDecodeError, TypeError):
                                message_data[key_str] = value_str
                        
                        # Create QueueMessage object
                        queue_message = QueueMessage.from_dict(message_data)
                        queue_message.id = message_id.decode() if isinstance(message_id, bytes) else message_id
                        result.append(queue_message)
            
            return result
            
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                await self.setup_consumer_group(consumer_name)
                return await self.consume(consumer_name, timeout, count)
            raise
    
    async def acknowledge(self, consumer_name: str, *message_ids: str) -> int:
        """Acknowledge processed messages."""
        if not self.redis_client._client:
            await self.redis_client.connect()
        
        return await self.redis_client._client.xack(
            self.queue_name,
            self.consumer_group,
            *message_ids
        )
    
    async def get_queue_length(self) -> int:
        """Get approximate queue length."""
        if not self.redis_client._client:
            await self.redis_client.connect()
        
        return await self.redis_client._client.xlen(self.queue_name)
    
    async def get_consumer_info(self) -> List[Dict]:
        """Get consumer group information."""
        if not self.redis_client._client:
            await self.redis_client.connect()
        
        try:
            info = await self.redis_client._client.xinfo_consumers(
                self.queue_name, 
                self.consumer_group
            )
            return info
        except redis.exceptions.ResponseError:
            return []


class QueueManager:
    """Manages multiple Redis queues with automatic failover and monitoring."""
    
    def __init__(self, redis_client: RedisClient):
        self.redis_client = redis_client
        self.queues: Dict[str, RedisQueue] = {}
        self._health_check_task: Optional[asyncio.Task] = None
    
    def get_queue(self, queue_name: str) -> RedisQueue:
        """Get or create a queue instance."""
        if queue_name not in self.queues:
            self.queues[queue_name] = RedisQueue(self.redis_client, queue_name)
        return self.queues[queue_name]
    
    async def publish_to_queue(
        self, 
        queue_name: str, 
        data: Dict[str, Any],
        priority: int = 1
    ) -> str:
        """Publish message to specified queue."""
        queue = self.get_queue(queue_name)
        return await queue.publish(data, priority=priority)
    
    async def consume_from_queue(
        self, 
        queue_name: str, 
        consumer_name: str = "default",
        timeout: int = 5000,
        count: int = 1
    ) -> List[QueueMessage]:
        """Consume messages from specified queue."""
        queue = self.get_queue(queue_name)
        return await queue.consume(consumer_name, timeout, count)
    
    async def acknowledge_messages(
        self, 
        queue_name: str, 
        consumer_name: str,
        *message_ids: str
    ) -> int:
        """Acknowledge processed messages."""
        queue = self.get_queue(queue_name)
        return await queue.acknowledge(consumer_name, *message_ids)
    
    async def get_queue_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all queues."""
        stats = {}
        for queue_name, queue in self.queues.items():
            stats[queue_name] = {
                "length": await queue.get_queue_length(),
                "consumers": await queue.get_consumer_info()
            }
        return stats
    
    async def start_health_monitoring(self, interval: int = 30):
        """Start background health monitoring."""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(
                self._health_check_loop(interval)
            )
    
    async def stop_health_monitoring(self):
        """Stop background health monitoring."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
    
    async def _health_check_loop(self, interval: int):
        """Background health check loop."""
        while True:
            try:
                await asyncio.sleep(interval)
                is_healthy = await self.redis_client.ping()
                if not is_healthy:
                    # Log health check failure
                    print(f"Redis health check failed at {datetime.utcnow()}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Health check error: {e}")


# Global Redis client and queue manager instances
_redis_client: Optional[RedisClient] = None
_queue_manager: Optional[QueueManager] = None


def get_redis_client() -> RedisClient:
    """Get global Redis client instance."""
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = RedisClient(
            redis_url=settings.redis_url,
            max_connections=settings.redis_max_connections
        )
    return _redis_client


def get_queue_manager() -> QueueManager:
    """Get global queue manager instance."""
    global _queue_manager
    if _queue_manager is None:
        redis_client = get_redis_client()
        _queue_manager = QueueManager(redis_client)
    return _queue_manager


# Utility functions for common queue operations
async def enqueue_job(queue_name: str, job_data: Dict[str, Any], priority: int = 1) -> str:
    """Convenience function to enqueue a job."""
    queue_manager = get_queue_manager()
    return await queue_manager.publish_to_queue(queue_name, job_data, priority)


async def dequeue_job(
    queue_name: str, 
    consumer_name: str = "default",
    timeout: int = 5000
) -> Optional[QueueMessage]:
    """Convenience function to dequeue a single job."""
    queue_manager = get_queue_manager()
    messages = await queue_manager.consume_from_queue(
        queue_name, consumer_name, timeout, count=1
    )
    return messages[0] if messages else None


async def ack_job(queue_name: str, consumer_name: str, message_id: str) -> bool:
    """Convenience function to acknowledge a job."""
    queue_manager = get_queue_manager()
    result = await queue_manager.acknowledge_messages(
        queue_name, consumer_name, message_id
    )
    return result > 0