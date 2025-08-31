"""
Utility functions and helpers for K-Scrape Nexus.
Common functionality shared across all services.
"""

import asyncio
import hashlib
import re
import urllib.parse
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timezone
from pathlib import Path
import mimetypes
import csv
import json
import pandas as pd
from urllib.robotparser import RobotFileParser
import validators

from .config import get_settings


class URLValidator:
    """URL validation and normalization utilities."""
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if URL is valid."""
        return validators.url(url) is True
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """Normalize URL for consistent processing."""
        # Parse URL
        parsed = urllib.parse.urlparse(url)
        
        # Ensure scheme
        if not parsed.scheme:
            url = f"https://{url}"
            parsed = urllib.parse.urlparse(url)
        
        # Normalize domain to lowercase
        domain = parsed.netloc.lower()
        
        # Remove default ports
        if domain.endswith(':80') and parsed.scheme == 'http':
            domain = domain[:-3]
        elif domain.endswith(':443') and parsed.scheme == 'https':
            domain = domain[:-4]
        
        # Normalize path
        path = parsed.path or '/'
        if not path.startswith('/'):
            path = f'/{path}'
        
        # Rebuild URL
        normalized = urllib.parse.urlunparse((
            parsed.scheme,
            domain,
            path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        return normalized
    
    @staticmethod
    def extract_domain(url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            parsed = urllib.parse.urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return None
    
    @staticmethod
    def is_same_domain(url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        domain1 = URLValidator.extract_domain(url1)
        domain2 = URLValidator.extract_domain(url2)
        return domain1 == domain2 if domain1 and domain2 else False


class RobotsTxtChecker:
    """Robots.txt checking with caching."""
    
    def __init__(self):
        self._cache: Dict[str, RobotFileParser] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_ttl = 3600  # 1 hour
    
    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if URL can be fetched according to robots.txt."""
        try:
            domain = URLValidator.extract_domain(url)
            if not domain:
                return True
            
            robots_url = f"{urllib.parse.urlparse(url).scheme}://{domain}/robots.txt"
            
            # Check cache
            if self._is_cached(robots_url):
                rp = self._cache[robots_url]
            else:
                # Fetch and parse robots.txt
                rp = RobotFileParser()
                rp.set_url(robots_url)
                try:
                    rp.read()
                    self._cache[robots_url] = rp
                    self._cache_timestamps[robots_url] = datetime.now(timezone.utc)
                except Exception:
                    # If robots.txt is not accessible, allow crawling
                    return True
            
            return rp.can_fetch(user_agent, url)
            
        except Exception:
            # If any error occurs, allow crawling
            return True
    
    def _is_cached(self, robots_url: str) -> bool:
        """Check if robots.txt is cached and still valid."""
        if robots_url not in self._cache:
            return False
        
        timestamp = self._cache_timestamps.get(robots_url)
        if not timestamp:
            return False
        
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
        return age < self._cache_ttl


class FileProcessor:
    """File processing utilities for different file formats."""
    
    @staticmethod
    def get_file_type(file_path: str) -> Optional[str]:
        """Get file type from path."""
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type
    
    @staticmethod
    def is_supported_file(file_path: str) -> bool:
        """Check if file type is supported."""
        settings = get_settings()
        extension = Path(file_path).suffix.lower()
        return extension in settings.supported_file_extensions
    
    @staticmethod
    async def extract_urls_from_file(file_path: str) -> List[str]:
        """Extract URLs from various file formats."""
        urls = []
        extension = Path(file_path).suffix.lower()
        
        try:
            if extension == '.txt':
                urls = await FileProcessor._extract_from_text(file_path)
            elif extension == '.csv':
                urls = await FileProcessor._extract_from_csv(file_path)
            elif extension == '.json':
                urls = await FileProcessor._extract_from_json(file_path)
            elif extension in ['.xlsx', '.xls']:
                urls = await FileProcessor._extract_from_excel(file_path)
        except Exception as e:
            raise ValueError(f"Failed to process file {file_path}: {str(e)}")
        
        # Validate and normalize URLs
        validated_urls = []
        for url in urls:
            if URLValidator.is_valid_url(url):
                validated_urls.append(URLValidator.normalize_url(url))
        
        return validated_urls
    
    @staticmethod
    async def _extract_from_text(file_path: str) -> List[str]:
        """Extract URLs from text file."""
        urls = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
        return urls
    
    @staticmethod
    async def _extract_from_csv(file_path: str) -> List[str]:
        """Extract URLs from CSV file."""
        urls = []
        with open(file_path, 'r', encoding='utf-8') as f:
            # Try to detect if first row is header
            sample = f.read(1024)
            f.seek(0)
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(sample)
            
            reader = csv.reader(f)
            if has_header:
                next(reader)  # Skip header
            
            for row in reader:
                if row:
                    # Take the first column as URL
                    urls.append(row[0].strip())
        
        return urls
    
    @staticmethod
    async def _extract_from_json(file_path: str) -> List[str]:
        """Extract URLs from JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        urls = []
        if isinstance(data, list):
            # Array of URLs or objects
            for item in data:
                if isinstance(item, str):
                    urls.append(item)
                elif isinstance(item, dict) and 'url' in item:
                    urls.append(item['url'])
        elif isinstance(data, dict) and 'urls' in data:
            # Object with urls array
            urls = data['urls']
        
        return urls
    
    @staticmethod
    async def _extract_from_excel(file_path: str) -> List[str]:
        """Extract URLs from Excel file."""
        df = pd.read_excel(file_path)
        
        # Take the first column
        if not df.empty:
            first_column = df.iloc[:, 0]
            urls = [str(url).strip() for url in first_column if pd.notna(url)]
            return urls
        
        return []


class DataCleaner:
    """Data cleaning and validation utilities."""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove control characters
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
        
        return text
    
    @staticmethod
    def extract_emails(text: str) -> List[str]:
        """Extract email addresses from text."""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text, re.IGNORECASE)
        return list(set(emails))  # Remove duplicates
    
    @staticmethod
    def extract_phone_numbers(text: str) -> List[str]:
        """Extract phone numbers from text."""
        # Various phone number patterns
        patterns = [
            r'\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',  # US format
            r'\+?(\d{1,3})[-.\s]?(\d{1,4})[-.\s]?(\d{1,4})[-.\s]?(\d{1,9})',  # International
            r'\b\d{3}-\d{3}-\d{4}\b',  # XXX-XXX-XXXX
            r'\b\(\d{3}\)\s?\d{3}-\d{4}\b',  # (XXX) XXX-XXXX
        ]
        
        phones = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    phone = ''.join(match)
                else:
                    phone = match
                phones.append(phone)
        
        return list(set(phones))
    
    @staticmethod
    def extract_links(html: str, base_url: str = "") -> List[str]:
        """Extract links from HTML content."""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('http'):
                links.append(href)
            elif base_url and not href.startswith('#'):
                # Convert relative URL to absolute
                absolute_url = urllib.parse.urljoin(base_url, href)
                links.append(absolute_url)
        
        return list(set(links))
    
    @staticmethod
    def validate_data_quality(data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and score data quality."""
        quality_score = 0.0
        completeness_score = 0.0
        
        required_fields = ['url', 'title', 'content']
        total_fields = len(required_fields)
        present_fields = 0
        
        for field in required_fields:
            if field in data and data[field]:
                present_fields += 1
                if field == 'title' and len(str(data[field])) > 10:
                    quality_score += 0.3
                elif field == 'content' and len(str(data[field])) > 100:
                    quality_score += 0.5
                elif field == 'url' and URLValidator.is_valid_url(str(data[field])):
                    quality_score += 0.2
        
        completeness_score = present_fields / total_fields
        
        return {
            'quality_score': min(quality_score, 1.0),
            'completeness_score': completeness_score,
            'present_fields': present_fields,
            'total_fields': total_fields
        }


class HashGenerator:
    """Content hashing utilities for deduplication."""
    
    @staticmethod
    def generate_content_hash(content: str) -> str:
        """Generate hash for content deduplication."""
        # Normalize content before hashing
        normalized = DataCleaner.clean_text(content).lower()
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    @staticmethod
    def generate_url_hash(url: str) -> str:
        """Generate hash for URL."""
        normalized_url = URLValidator.normalize_url(url)
        return hashlib.md5(normalized_url.encode()).hexdigest()


class RateLimiter:
    """Async rate limiting utility."""
    
    def __init__(self, requests_per_second: float, burst: int = 10):
        self.rate = requests_per_second
        self.burst = burst
        self.tokens = burst
        self.last_update = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> bool:
        """Acquire a token for rate limiting."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            time_passed = now - self.last_update
            self.last_update = now
            
            # Add tokens based on time passed
            self.tokens = min(self.burst, self.tokens + time_passed * self.rate)
            
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            
            return False
    
    async def wait_for_token(self):
        """Wait until a token is available."""
        while not await self.acquire():
            await asyncio.sleep(0.1)


class RetryHandler:
    """Async retry handler with exponential backoff."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
    
    async def execute(self, func, *args, **kwargs):
        """Execute function with retry logic."""
        last_exception = None
        
        for attempt in range(self.max_attempts):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt == self.max_attempts - 1:
                    # Last attempt, re-raise exception
                    raise
                
                # Calculate delay with exponential backoff
                delay = min(
                    self.base_delay * (self.backoff_factor ** attempt),
                    self.max_delay
                )
                
                await asyncio.sleep(delay)
        
        # This should never be reached, but just in case
        if last_exception:
            raise last_exception


# Utility functions
def generate_task_id() -> str:
    """Generate unique task ID."""
    import uuid
    return str(uuid.uuid4())


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"


def truncate_string(text: str, max_length: int = 100) -> str:
    """Truncate string to maximum length."""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def safe_filename(filename: str) -> str:
    """Create safe filename by removing invalid characters."""
    # Remove or replace invalid characters
    safe_chars = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing spaces and dots
    safe_chars = safe_chars.strip(' .')
    # Limit length
    return safe_chars[:255] if safe_chars else "unnamed"