#!/usr/bin/env python3
"""
Test environment setup script.
Sets up the necessary environment variables for testing.
"""

import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'services', 'worker-shared'))

# Set test environment variables
os.environ.update({
    'DATABASE_URL': 'sqlite+aiosqlite:///:memory:',
    'REDIS_URL': 'redis://localhost:6379/1', 
    'S3_BUCKET': 'test-bucket',
    'S3_ENDPOINT_URL': 'http://localhost:9000',
    'AWS_ACCESS_KEY_ID': 'testkey',
    'AWS_SECRET_ACCESS_KEY': 'testsecret',
    'SECRET_KEY': 'test-secret-key-for-testing-only',
    'DEBUG': 'true',
    'LOG_LEVEL': 'DEBUG',
    'METRICS_ENABLED': 'false',
    'TRACING_ENABLED': 'false'
})

if __name__ == "__main__":
    print("Test environment configured successfully")
    print(f"DATABASE_URL: {os.environ['DATABASE_URL']}")
    print(f"REDIS_URL: {os.environ['REDIS_URL']}")