"""
Data Processing Worker with Pandas for data cleaning and transformation.
Handles post-scraping data quality improvement, normalization, and enrichment.
"""

import asyncio
import json
import re
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from urllib.parse import urlparse

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import structlog

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from scraper_lib import get_settings, get_database_session, get_redis_client
from scraper_lib.models import Task, TaskStatus, ScrapingResult
from scraper_lib.observability import get_metrics_collector

logger = structlog.get_logger(__name__)


@dataclass
class ProcessedData:
    """Container for processed data results."""
    original_url: str
    cleaned_title: str
    cleaned_content: str
    extracted_entities: Dict[str, List[str]]
    structured_data: Dict[str, Any]
    quality_metrics: Dict[str, float]
    processing_metadata: Dict[str, Any]
    success: bool = True
    error_message: Optional[str] = None


class DataProcessor:
    """
    Advanced data processing and cleaning using Pandas and ML techniques.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.metrics = get_metrics_collector("data-processor")
        
        # Text cleaning patterns
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.phone_pattern = re.compile(r'(\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})')
        self.url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        self.social_handles = re.compile(r'@([A-Za-z0-9_]+)')
        
        # Stop words for content cleaning
        self.stop_words = {
            'english': {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'up', 'down', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once'}
        }
    
    async def process_scraped_data(self, raw_data: Dict[str, Any]) -> ProcessedData:
        """
        Process raw scraped data through various cleaning and enhancement steps.
        
        Args:
            raw_data: Raw scraped data from scraping workers
            
        Returns:
            ProcessedData object with cleaned and enhanced data
        """
        start_time = time.time()
        
        try:
            logger.info(f"Processing data for URL: {raw_data.get('url', 'Unknown')}")
            
            # Step 1: Basic data validation
            if not self._validate_input_data(raw_data):
                raise ValueError("Invalid input data structure")
            
            # Step 2: Extract and clean text content
            cleaned_content = await self._clean_text_content(raw_data.get('content', ''))
            cleaned_title = await self._clean_title(raw_data.get('title', ''))
            
            # Step 3: Extract structured entities
            entities = await self._extract_entities(cleaned_content, raw_data)
            
            # Step 4: Parse and structure HTML content
            structured_data = await self._extract_structured_data(raw_data.get('html', ''))
            
            # Step 5: Enhance with metadata
            enhanced_data = await self._enhance_with_metadata(raw_data, entities)
            
            # Step 6: Calculate quality metrics
            quality_metrics = await self._calculate_quality_metrics(raw_data, cleaned_content, entities)
            
            # Step 7: Detect content type and categorize
            content_analysis = await self._analyze_content_type(cleaned_content, structured_data)
            
            # Create processed result
            result = ProcessedData(
                original_url=raw_data['url'],
                cleaned_title=cleaned_title,
                cleaned_content=cleaned_content,
                extracted_entities=entities,
                structured_data={**structured_data, **content_analysis},
                quality_metrics=quality_metrics,
                processing_metadata={
                    'processing_time': time.time() - start_time,
                    'processing_timestamp': datetime.now(timezone.utc).isoformat(),
                    'enhancements_applied': enhanced_data,
                    'original_data_size': len(str(raw_data)),
                    'processed_data_size': len(cleaned_content)
                }
            )
            
            # Record success metrics
            self.metrics.data_processing_total.labels(status="success").inc()
            self.metrics.processing_duration.observe(time.time() - start_time)
            
            return result
            
        except Exception as e:
            error_message = f"Data processing failed: {str(e)}"
            logger.error(error_message)
            
            # Record error metrics
            self.metrics.data_processing_total.labels(status="error").inc()
            self.metrics.processing_errors_total.labels(error_type=type(e).__name__).inc()
            
            return ProcessedData(
                original_url=raw_data.get('url', 'Unknown'),
                cleaned_title='',
                cleaned_content='',
                extracted_entities={},
                structured_data={},
                quality_metrics={},
                processing_metadata={
                    'processing_time': time.time() - start_time,
                    'error_message': error_message
                },
                success=False,
                error_message=error_message
            )
    
    def _validate_input_data(self, data: Dict[str, Any]) -> bool:
        """Validate input data structure."""
        required_fields = ['url']
        return all(field in data for field in required_fields)
    
    async def _clean_text_content(self, content: str) -> str:
        """Clean and normalize text content."""
        if not content:
            return ""
        
        # Remove excessive whitespace
        content = re.sub(r'\s+', ' ', content)
        
        # Remove special characters but keep punctuation
        content = re.sub(r'[^\w\s.,!?;:()\-"]', '', content)
        
        # Fix common encoding issues
        content = content.replace('\u00a0', ' ')  # Non-breaking space
        content = content.replace('\u2019', "'")  # Smart quote
        content = content.replace('\u201c', '"')  # Smart quote
        content = content.replace('\u201d', '"')  # Smart quote
        
        # Remove repeated punctuation
        content = re.sub(r'([.!?]){2,}', r'\1', content)
        
        # Trim whitespace
        content = content.strip()
        
        return content
    
    async def _clean_title(self, title: str) -> str:
        """Clean and normalize page title."""
        if not title:
            return ""
        
        # Remove common title suffixes
        common_suffixes = [' - Homepage', ' | Home', ' - Home', ' - Official Site', ' - Official Website']
        for suffix in common_suffixes:
            if title.endswith(suffix):
                title = title[:-len(suffix)]
        
        # Clean whitespace and special characters
        title = re.sub(r'\s+', ' ', title).strip()
        title = re.sub(r'[^\w\s.,!?()\-"]', '', title)
        
        return title
    
    async def _extract_entities(self, content: str, raw_data: Dict) -> Dict[str, List[str]]:
        """Extract various entities from content."""
        entities = {
            'emails': [],
            'phone_numbers': [],
            'urls': [],
            'social_handles': [],
            'companies': [],
            'locations': [],
            'keywords': []
        }
        
        # Extract emails
        entities['emails'] = list(set(self.email_pattern.findall(content)))
        
        # Extract phone numbers
        phone_matches = self.phone_pattern.findall(content)
        entities['phone_numbers'] = ['-'.join(match[1:]) for match in phone_matches]
        
        # Extract URLs
        entities['urls'] = list(set(self.url_pattern.findall(content)))
        
        # Extract social handles
        entities['social_handles'] = list(set(self.social_handles.findall(content)))
        
        # Extract potential keywords (simple frequency-based approach)
        entities['keywords'] = await self._extract_keywords(content)
        
        # Extract company names (simple heuristic-based approach)
        entities['companies'] = await self._extract_company_names(content)
        
        # Extract locations (basic pattern matching)
        entities['locations'] = await self._extract_locations(content)
        
        return entities
    
    async def _extract_keywords(self, content: str, max_keywords: int = 20) -> List[str]:
        """Extract keywords using frequency analysis."""
        if not content:
            return []
        
        # Convert to lowercase and split into words
        words = re.findall(r'\b[a-zA-Z]{3,}\b', content.lower())
        
        # Remove stop words
        stop_words = self.stop_words.get('english', set())
        words = [word for word in words if word not in stop_words]
        
        # Use pandas for frequency analysis
        if words:
            word_series = pd.Series(words)
            word_counts = word_series.value_counts()
            return word_counts.head(max_keywords).index.tolist()
        
        return []
    
    async def _extract_company_names(self, content: str) -> List[str]:
        """Extract potential company names using patterns."""
        # Simple pattern for company names
        company_patterns = [
            r'\b([A-Z][a-z]+ (?:Inc|LLC|Corp|Corporation|Company|Co|Ltd)\.?)\b',
            r'\b([A-Z][a-z]+ & [A-Z][a-z]+)\b',
            r'\b([A-Z]{2,})\b'  # Acronyms
        ]
        
        companies = []
        for pattern in company_patterns:
            matches = re.findall(pattern, content)
            companies.extend(matches)
        
        # Filter and deduplicate
        companies = list(set([company for company in companies if len(company) > 2]))
        return companies[:10]  # Limit to top 10
    
    async def _extract_locations(self, content: str) -> List[str]:
        """Extract potential locations using patterns."""
        # Basic patterns for locations
        location_patterns = [
            r'\b([A-Z][a-z]+, [A-Z]{2})\b',  # City, State
            r'\b([A-Z][a-z]+ [A-Z][a-z]+, [A-Z]{2})\b',  # City City, State
            r'\b(\d{5}(?:-\d{4})?)\b'  # ZIP codes
        ]
        
        locations = []
        for pattern in location_patterns:
            matches = re.findall(pattern, content)
            locations.extend(matches)
        
        return list(set(locations))[:10]  # Limit and deduplicate
    
    async def _extract_structured_data(self, html_content: str) -> Dict[str, Any]:
        """Extract structured data from HTML."""
        structured_data = {
            'headings': {'h1': [], 'h2': [], 'h3': []},
            'lists': [],
            'tables': [],
            'forms': [],
            'schema_org': {},
            'open_graph': {},
            'twitter_cards': {}
        }
        
        if not html_content:
            return structured_data
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract headings
            for level in ['h1', 'h2', 'h3']:
                headings = soup.find_all(level)
                structured_data['headings'][level] = [h.get_text().strip() for h in headings]
            
            # Extract lists
            lists = soup.find_all(['ul', 'ol'])
            for lst in lists:
                items = [li.get_text().strip() for li in lst.find_all('li')]
                if items:
                    structured_data['lists'].append({
                        'type': lst.name,
                        'items': items
                    })
            
            # Extract tables
            tables = soup.find_all('table')
            for table in tables:
                rows = []
                for row in table.find_all('tr'):
                    cells = [cell.get_text().strip() for cell in row.find_all(['td', 'th'])]
                    if cells:
                        rows.append(cells)
                if rows:
                    structured_data['tables'].append(rows)
            
            # Extract form information
            forms = soup.find_all('form')
            for form in forms:
                inputs = form.find_all(['input', 'select', 'textarea'])
                form_data = {
                    'action': form.get('action', ''),
                    'method': form.get('method', 'get'),
                    'fields': [{'type': inp.get('type', inp.name), 'name': inp.get('name', '')} for inp in inputs]
                }
                structured_data['forms'].append(form_data)
            
            # Extract Schema.org data
            schema_scripts = soup.find_all('script', type='application/ld+json')
            for script in schema_scripts:
                try:
                    schema_data = json.loads(script.string)
                    structured_data['schema_org'].update(schema_data)
                except:
                    pass
            
            # Extract Open Graph tags
            og_tags = soup.find_all('meta', property=lambda x: x and x.startswith('og:'))
            for tag in og_tags:
                prop = tag.get('property', '').replace('og:', '')
                content = tag.get('content', '')
                if prop and content:
                    structured_data['open_graph'][prop] = content
            
            # Extract Twitter Card tags
            twitter_tags = soup.find_all('meta', attrs={'name': lambda x: x and x.startswith('twitter:')})
            for tag in twitter_tags:
                name = tag.get('name', '').replace('twitter:', '')
                content = tag.get('content', '')
                if name and content:
                    structured_data['twitter_cards'][name] = content
                    
        except Exception as e:
            logger.warning(f"Error extracting structured data: {e}")
        
        return structured_data
    
    async def _enhance_with_metadata(self, raw_data: Dict, entities: Dict) -> Dict[str, Any]:
        """Enhance data with additional metadata."""
        enhancements = {}
        
        # URL analysis
        url = raw_data.get('url', '')
        if url:
            parsed_url = urlparse(url)
            enhancements['url_analysis'] = {
                'domain': parsed_url.netloc,
                'path_depth': len([p for p in parsed_url.path.split('/') if p]),
                'has_query_params': bool(parsed_url.query),
                'is_secure': parsed_url.scheme == 'https'
            }
        
        # Content statistics
        content = raw_data.get('content', '')
        enhancements['content_stats'] = {
            'word_count': len(content.split()) if content else 0,
            'char_count': len(content),
            'paragraph_count': content.count('\n\n') + 1 if content else 0,
            'reading_time_minutes': max(1, len(content.split()) // 200) if content else 0
        }
        
        # Entity statistics
        enhancements['entity_stats'] = {
            entity_type: len(entity_list) 
            for entity_type, entity_list in entities.items()
        }
        
        # Language detection (simple heuristic)
        enhancements['language'] = await self._detect_language(content)
        
        return enhancements
    
    async def _detect_language(self, content: str) -> str:
        """Simple language detection based on common words."""
        if not content:
            return 'unknown'
        
        # Simple heuristic - count common English words
        english_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = set(content.lower().split())
        english_count = len(words.intersection(english_words))
        
        if english_count > 5:
            return 'en'
        else:
            return 'unknown'
    
    async def _analyze_content_type(self, content: str, structured_data: Dict) -> Dict[str, Any]:
        """Analyze and categorize content type."""
        analysis = {
            'content_type': 'article',  # Default
            'is_product_page': False,
            'is_contact_page': False,
            'is_news_article': False,
            'is_blog_post': False,
            'confidence_score': 0.0
        }
        
        # Check for product page indicators
        product_indicators = ['price', 'buy', 'cart', 'product', 'purchase', 'shipping']
        product_score = sum(1 for indicator in product_indicators if indicator in content.lower())
        
        # Check for contact page indicators
        contact_indicators = ['contact', 'email', 'phone', 'address', 'location']
        contact_score = sum(1 for indicator in contact_indicators if indicator in content.lower())
        
        # Check for news/blog indicators
        news_indicators = ['published', 'author', 'date', 'news', 'article', 'blog']
        news_score = sum(1 for indicator in news_indicators if indicator in content.lower())
        
        # Determine content type based on scores
        max_score = max(product_score, contact_score, news_score)
        
        if max_score > 0:
            if product_score == max_score:
                analysis['content_type'] = 'product'
                analysis['is_product_page'] = True
            elif contact_score == max_score:
                analysis['content_type'] = 'contact'
                analysis['is_contact_page'] = True
            elif news_score == max_score:
                analysis['content_type'] = 'news'
                analysis['is_news_article'] = True
            
            analysis['confidence_score'] = min(max_score / 10.0, 1.0)
        
        return analysis
    
    async def _calculate_quality_metrics(self, raw_data: Dict, cleaned_content: str, entities: Dict) -> Dict[str, float]:
        """Calculate various quality metrics for the processed data."""
        metrics = {}
        
        # Content completeness
        original_content = raw_data.get('content', '')
        if original_content:
            metrics['content_retention'] = len(cleaned_content) / len(original_content)
        else:
            metrics['content_retention'] = 0.0
        
        # Information density
        word_count = len(cleaned_content.split()) if cleaned_content else 0
        total_entities = sum(len(entity_list) for entity_list in entities.values())
        metrics['information_density'] = total_entities / max(word_count, 1)
        
        # Content quality score
        quality_factors = [
            min(word_count / 100, 1.0),  # Word count factor
            min(len(entities.get('keywords', [])) / 10, 1.0),  # Keyword diversity
            1.0 if entities.get('emails') or entities.get('phone_numbers') else 0.0,  # Contact info
            min(len(raw_data.get('links', [])) / 5, 1.0) if raw_data.get('links') else 0.0  # Link factor
        ]
        metrics['overall_quality'] = sum(quality_factors) / len(quality_factors)
        
        # Data richness
        metrics['entity_richness'] = len([k for k, v in entities.items() if v]) / len(entities)
        
        # Processing effectiveness
        metrics['processing_improvement'] = max(0, metrics['overall_quality'] - 0.5) * 2
        
        return metrics
    
    async def process_task(self, task_data: Dict[str, Any]):
        """
        Process a single data processing task.
        
        Args:
            task_data: Dictionary containing task information
        """
        task_id = task_data.get('task_id')
        job_id = task_data.get('job_id')
        result_location = task_data.get('result_location')
        
        logger.info(f"Processing data task {task_id} from {result_location}")
        
        try:
            # Update task status to in progress
            await self._update_task_status(task_id, TaskStatus.IN_PROGRESS)
            
            # Load raw data from storage
            raw_data = await self._load_raw_data(result_location)
            
            # Process the data
            processed_result = await self.process_scraped_data(raw_data)
            
            # Store processed result
            if processed_result.success:
                processed_location = await self._store_processed_data(processed_result)
                
                # Update database with processed data
                await self._update_database_with_processed_data(task_id, processed_result, processed_location)
                
                await self._update_task_status(
                    task_id, 
                    TaskStatus.COMPLETED,
                    result_location=processed_location
                )
                
                logger.info(f"Completed data processing task {task_id}")
                
            else:
                await self._update_task_status(
                    task_id, 
                    TaskStatus.FAILED, 
                    error_message=processed_result.error_message
                )
            
        except Exception as e:
            error_message = f"Data processing task failed: {str(e)}"
            logger.error(error_message)
            
            await self._update_task_status(
                task_id, 
                TaskStatus.FAILED, 
                error_message=error_message
            )
    
    async def _load_raw_data(self, result_location: str) -> Dict[str, Any]:
        """Load raw data from storage location."""
        # Implementation would load from S3 or other storage
        # For now, return mock data
        return {
            'url': 'https://example.com',
            'title': 'Example Page',
            'content': 'Sample content for processing',
            'html': '<html><body>Sample HTML</body></html>',
            'links': ['https://example.com/link1'],
            'metadata': {}
        }
    
    async def _store_processed_data(self, processed_data: ProcessedData) -> str:
        """Store processed data to storage."""
        # Implementation would store to S3 or database
        # Return the storage location
        return f"s3://bucket/processed-data/{int(time.time())}.json"
    
    async def _update_database_with_processed_data(self, task_id: str, processed_data: ProcessedData, location: str):
        """Update database with processed data results."""
        # Implementation would update the Result table with processed data
        logger.info(f"Updated database for task {task_id} with processed data")
    
    async def _update_task_status(self, task_id: str, status: TaskStatus, 
                                result_location: Optional[str] = None,
                                error_message: Optional[str] = None):
        """Update task status in database."""
        async with get_database_session() as session:
            # Implementation would update the task in database
            logger.info(f"Updated task {task_id} status to {status}")


class DataProcessingWorker:
    """Main worker class for data processing."""
    
    def __init__(self):
        self.settings = get_settings()
        self.processor = DataProcessor()
        self.redis_client = get_redis_client()
        self.running = False
        
    async def start(self):
        """Start the data processing worker."""
        try:
            self.running = True
            logger.info("Data Processing Worker started")
            
            # Start consuming tasks
            await self._consume_tasks()
            
        except Exception as e:
            logger.error(f"Failed to start worker: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the worker."""
        self.running = False
        logger.info("Data Processing Worker stopped")
    
    async def _consume_tasks(self):
        """Consume tasks from Redis queue."""
        queue_name = "needs_cleaning"
        consumer_group = "processing_workers"
        consumer_name = f"worker_{os.getpid()}"
        
        # Create consumer group if it doesn't exist
        try:
            await self.redis_client.xgroup_create(queue_name, consumer_group, id="0", mkstream=True)
        except:
            pass  # Group already exists
        
        while self.running:
            try:
                # Read from queue
                messages = await self.redis_client.xreadgroup(
                    consumer_group,
                    consumer_name,
                    {queue_name: ">"},
                    count=1,
                    block=1000
                )
                
                for stream, msgs in messages:
                    for msg_id, fields in msgs:
                        try:
                            # Process the task
                            await self.processor.process_task(fields)
                            
                            # Acknowledge the message
                            await self.redis_client.xack(queue_name, consumer_group, msg_id)
                            
                        except Exception as e:
                            logger.error(f"Error processing message {msg_id}: {e}")
                            
            except Exception as e:
                logger.error(f"Error consuming tasks: {e}")
                await asyncio.sleep(5)


# Main execution
async def main():
    """Main entry point for the data processing worker."""
    worker = DataProcessingWorker()
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())