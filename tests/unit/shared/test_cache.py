"""
Unit tests for shared library cache and Redis operations.
"""

import pytest
import json
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from scraper_lib.cache import RedisClient, RedisQueue, QueueMessage, QueueManager


class TestRedisClient:
    """Test Redis client functionality."""
    
    @pytest.fixture
    def mock_redis_pool(self):
        """Mock Redis connection pool."""
        mock_pool = MagicMock()
        mock_redis = AsyncMock()
        
        # Mock basic operations
        mock_redis.ping.return_value = True
        mock_redis.set.return_value = True
        mock_redis.get.return_value = b'{"test": "value"}'
        mock_redis.delete.return_value = 1
        mock_redis.exists.return_value = True
        mock_redis.expire.return_value = True
        mock_redis.incr.return_value = 1
        mock_redis.hset.return_value = 1
        mock_redis.hget.return_value = b'test_value'
        mock_redis.hgetall.return_value = {b'key1': b'value1', b'key2': b'value2'}
        
        mock_pool.disconnect = AsyncMock()
        return mock_pool, mock_redis
    
    async def test_redis_client_connection(self, mock_redis_pool):
        """Test Redis client connection."""
        mock_pool, mock_redis = mock_redis_pool
        
        client = RedisClient("redis://localhost:6379")
        client._pool = mock_pool
        client._client = mock_redis
        
        # Test ping
        result = await client.ping()
        assert result is True
        mock_redis.ping.assert_called_once()
    
    async def test_set_and_get_operations(self, mock_redis_pool):
        """Test basic set and get operations."""
        mock_pool, mock_redis = mock_redis_pool
        
        client = RedisClient("redis://localhost:6379")
        client._client = mock_redis
        
        # Test set operation
        result = await client.set("test_key", {"test": "value"})
        assert result is True
        mock_redis.set.assert_called_once_with("test_key", '{"test": "value"}', ex=None)
        
        # Test get operation
        value = await client.get("test_key")
        assert value == {"test": "value"}
        mock_redis.get.assert_called_once_with("test_key")
    
    async def test_hash_operations(self, mock_redis_pool):
        """Test Redis hash operations."""
        mock_pool, mock_redis = mock_redis_pool
        
        client = RedisClient("redis://localhost:6379")
        client._client = mock_redis
        
        # Test hset
        mapping = {"field1": "value1", "field2": {"nested": "value"}}
        result = await client.hset("test_hash", mapping)
        assert result == 1
        
        # Test hget
        mock_redis.hget.return_value = b'"value1"'
        value = await client.hget("test_hash", "field1")
        assert value == "value1"
        
        # Test hgetall
        mock_redis.hgetall.return_value = {
            b'field1': b'"value1"',
            b'field2': b'{"nested": "value"}'
        }
        all_values = await client.hgetall("test_hash")
        assert all_values == {"field1": "value1", "field2": {"nested": "value"}}


class TestQueueMessage:
    """Test QueueMessage functionality."""
    
    def test_queue_message_creation(self):
        """Test creating a queue message."""
        message_id = str(uuid.uuid4())
        data = {"task_id": "123", "url": "https://example.com"}
        timestamp = datetime.utcnow()
        
        message = QueueMessage(
            id=message_id,
            data=data,
            timestamp=timestamp,
            retry_count=0,
            max_retries=3
        )
        
        assert message.id == message_id
        assert message.data == data
        assert message.timestamp == timestamp
        assert message.retry_count == 0
        assert message.max_retries == 3
    
    def test_queue_message_serialization(self):
        """Test message serialization to/from dict."""
        message_id = str(uuid.uuid4())
        data = {"task_id": "123", "url": "https://example.com"}
        timestamp = datetime.utcnow()
        
        message = QueueMessage(
            id=message_id,
            data=data,
            timestamp=timestamp,
            retry_count=1,
            max_retries=3
        )
        
        # Test to_dict
        message_dict = message.to_dict()
        assert message_dict["id"] == message_id
        assert message_dict["data"] == data
        assert message_dict["retry_count"] == 1
        assert message_dict["max_retries"] == 3
        assert "timestamp" in message_dict
        
        # Test from_dict
        reconstructed = QueueMessage.from_dict(message_dict)
        assert reconstructed.id == message.id
        assert reconstructed.data == message.data
        assert reconstructed.retry_count == message.retry_count
        assert reconstructed.max_retries == message.max_retries


class TestRedisQueue:
    """Test Redis queue functionality."""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client for queue testing."""
        mock_client = AsyncMock()
        
        # Mock Redis Stream operations
        mock_client._client = AsyncMock()
        mock_client._client.xgroup_create = AsyncMock()
        mock_client._client.xadd = AsyncMock(return_value=b"1234567890-0")
        mock_client._client.xreadgroup = AsyncMock()
        mock_client._client.xack = AsyncMock(return_value=1)
        mock_client._client.xlen = AsyncMock(return_value=5)
        mock_client._client.xinfo_consumers = AsyncMock(return_value=[])
        
        return mock_client
    
    async def test_queue_publish(self, mock_redis_client):
        """Test publishing messages to queue."""
        queue = RedisQueue(mock_redis_client, "test_queue")
        
        data = {"task_id": "123", "url": "https://example.com"}
        
        # Test publish
        message_id = await queue.publish(data, priority=1)
        assert message_id == "1234567890-0"
        
        # Verify xadd was called
        mock_redis_client._client.xadd.assert_called_once()
        call_args = mock_redis_client._client.xadd.call_args
        assert call_args[0][0] == "test_queue"  # queue name
    
    async def test_queue_consume(self, mock_redis_client):
        """Test consuming messages from queue."""
        queue = RedisQueue(mock_redis_client, "test_queue")
        
        # Mock xreadgroup response
        mock_redis_client._client.xreadgroup.return_value = [
            [b"test_queue", [
                [b"1234567890-0", {
                    b"id": b"msg-123",
                    b"data": b'{"task_id": "123"}',
                    b"timestamp": b"2023-01-01T00:00:00",
                    b"retry_count": b"0",
                    b"max_retries": b"3",
                    b"priority": b"1"
                }]
            ]]
        ]
        
        # Test consume
        messages = await queue.consume("consumer1", timeout=5000, count=1)
        
        assert len(messages) == 1
        message = messages[0]
        assert message.id == "1234567890-0"
        assert message.data == {"task_id": "123"}
        assert message.retry_count == 0
    
    async def test_queue_acknowledge(self, mock_redis_client):
        """Test acknowledging processed messages."""
        queue = RedisQueue(mock_redis_client, "test_queue")
        
        # Test acknowledge
        result = await queue.acknowledge("consumer1", "1234567890-0")
        assert result == 1
        
        # Verify xack was called
        mock_redis_client._client.xack.assert_called_once_with(
            "test_queue", 
            "test_queue_consumers", 
            "1234567890-0"
        )
    
    async def test_queue_length(self, mock_redis_client):
        """Test getting queue length."""
        queue = RedisQueue(mock_redis_client, "test_queue")
        
        length = await queue.get_queue_length()
        assert length == 5
        
        mock_redis_client._client.xlen.assert_called_once_with("test_queue")


class TestQueueManager:
    """Test queue manager functionality."""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client for manager testing."""
        return AsyncMock()
    
    async def test_queue_manager_operations(self, mock_redis_client):
        """Test queue manager operations."""
        manager = QueueManager(mock_redis_client)
        
        # Mock queue
        mock_queue = AsyncMock()
        mock_queue.publish.return_value = "msg-123"
        mock_queue.consume.return_value = [MagicMock()]
        mock_queue.acknowledge.return_value = 1
        
        # Replace get_queue method
        manager.get_queue = MagicMock(return_value=mock_queue)
        
        # Test publish
        message_id = await manager.publish_to_queue(
            "test_queue", 
            {"test": "data"}, 
            priority=1
        )
        assert message_id == "msg-123"
        mock_queue.publish.assert_called_once_with({"test": "data"}, priority=1)
        
        # Test consume
        messages = await manager.consume_from_queue(
            "test_queue",
            "consumer1",
            timeout=5000,
            count=1
        )
        assert len(messages) == 1
        mock_queue.consume.assert_called_once_with("consumer1", 5000, 1)
        
        # Test acknowledge
        ack_result = await manager.acknowledge_messages(
            "test_queue",
            "consumer1", 
            "msg-123"
        )
        assert ack_result == 1
        mock_queue.acknowledge.assert_called_once_with("consumer1", "msg-123")
    
    async def test_queue_stats(self, mock_redis_client):
        """Test getting queue statistics."""
        manager = QueueManager(mock_redis_client)
        
        # Mock queue stats
        mock_queue = AsyncMock()
        mock_queue.get_queue_length.return_value = 10
        mock_queue.get_consumer_info.return_value = [
            {"name": "consumer1", "pending": 2, "idle": 1000}
        ]
        
        manager.queues = {"test_queue": mock_queue}
        
        stats = await manager.get_queue_stats()
        
        assert "test_queue" in stats
        assert stats["test_queue"]["length"] == 10
        assert len(stats["test_queue"]["consumers"]) == 1