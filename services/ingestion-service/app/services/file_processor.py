"""
File processor for extracting URLs from various file formats.
"""

import asyncio
import csv
import json
import io
from typing import List, Dict, Any
from pathlib import Path
import structlog

import pandas as pd
from scraper_lib.utils import URLValidator, FileProcessor as BaseFileProcessor
from scraper_lib.observability import monitor_function

logger = structlog.get_logger("ingestion-service.file_processor")


class FileProcessor:
    """Enhanced file processor for the ingestion service."""
    
    def __init__(self):
        self.base_processor = BaseFileProcessor()
        self.supported_formats = {
            '.txt': self.process_text_file,
            '.csv': self.process_csv_file,
            '.json': self.process_json_file,
            '.xlsx': self.process_excel_file,
            '.xls': self.process_excel_file
        }
    
    @monitor_function("extract_urls_from_content")
    async def extract_urls_from_content(self, content: bytes, file_path: str) -> List[str]:
        """Extract URLs from file content based on file type."""
        logger.info("Extracting URLs from file content", file_path=file_path)
        
        try:
            # Determine file type
            file_extension = Path(file_path).suffix.lower()
            
            if file_extension not in self.supported_formats:
                raise ValueError(f"Unsupported file format: {file_extension}")
            
            # Process file based on type
            processor = self.supported_formats[file_extension]
            urls = await processor(content)
            
            # Validate and clean URLs
            cleaned_urls = await self.validate_and_clean_urls(urls)
            
            logger.info(
                "URL extraction completed",
                file_path=file_path,
                raw_urls=len(urls),
                valid_urls=len(cleaned_urls)
            )
            
            return cleaned_urls
            
        except Exception as e:
            logger.error("Failed to extract URLs", file_path=file_path, error=str(e))
            raise
    
    async def process_text_file(self, content: bytes) -> List[str]:
        """Process plain text file containing URLs."""
        text_content = content.decode('utf-8', errors='ignore')
        urls = []
        
        for line in text_content.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                # Handle multiple URLs per line (comma or space separated)
                if ',' in line:
                    urls.extend([url.strip() for url in line.split(',')])
                elif ' ' in line and not line.startswith('http'):
                    # Multiple space-separated URLs
                    potential_urls = line.split()
                    urls.extend([url for url in potential_urls if 'http' in url])
                else:
                    urls.append(line)
        
        return urls
    
    async def process_csv_file(self, content: bytes) -> List[str]:
        """Process CSV file containing URLs."""
        text_content = content.decode('utf-8', errors='ignore')
        urls = []
        
        # Use StringIO to create file-like object
        csv_file = io.StringIO(text_content)
        
        # Try to detect CSV format
        try:
            # Sample first few lines to detect format
            sample = text_content[:1024]
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            has_header = sniffer.has_header(sample)
            
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            
            # Skip header if detected
            if has_header:
                next(reader, None)
            
            for row in reader:
                if row:
                    # Handle different CSV structures
                    if len(row) == 1:
                        # Single column with URLs
                        urls.append(row[0].strip())
                    else:
                        # Multiple columns - look for URL-like content
                        for cell in row:
                            cell = cell.strip()
                            if cell and ('http' in cell or '.' in cell):
                                urls.append(cell)
                                break  # Take first URL-like content per row
        
        except Exception as e:
            logger.warning("CSV parsing failed, trying simple processing", error=str(e))
            # Fallback: treat as comma-separated text
            for line in text_content.splitlines():
                if line.strip():
                    urls.extend([url.strip() for url in line.split(',') if url.strip()])
        
        return urls
    
    async def process_json_file(self, content: bytes) -> List[str]:
        """Process JSON file containing URLs."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            data = json.loads(text_content)
            urls = []
            
            if isinstance(data, list):
                # Array of URLs or objects
                for item in data:
                    if isinstance(item, str):
                        urls.append(item)
                    elif isinstance(item, dict):
                        # Look for URL fields
                        url_fields = ['url', 'link', 'href', 'address', 'uri']
                        for field in url_fields:
                            if field in item and item[field]:
                                urls.append(str(item[field]))
                                break
            
            elif isinstance(data, dict):
                # Object with URL arrays or nested structure
                url_arrays = ['urls', 'links', 'addresses', 'sites']
                
                for key in url_arrays:
                    if key in data and isinstance(data[key], list):
                        for url in data[key]:
                            if isinstance(url, str):
                                urls.append(url)
                            elif isinstance(url, dict) and 'url' in url:
                                urls.append(str(url['url']))
                
                # Also check for direct URL fields
                if not urls:
                    url_fields = ['url', 'link', 'href', 'address', 'uri']
                    for field in url_fields:
                        if field in data and data[field]:
                            if isinstance(data[field], str):
                                urls.append(data[field])
                            elif isinstance(data[field], list):
                                urls.extend([str(u) for u in data[field] if u])
            
            return urls
            
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format", error=str(e))
            raise ValueError(f"Invalid JSON format: {str(e)}")
    
    async def process_excel_file(self, content: bytes) -> List[str]:
        """Process Excel file containing URLs."""
        try:
            # Use BytesIO to create file-like object for pandas
            excel_file = io.BytesIO(content)
            
            # Read Excel file
            df = pd.read_excel(excel_file, sheet_name=None)  # Read all sheets
            urls = []
            
            # Process each sheet
            for sheet_name, sheet_df in df.items():
                logger.debug("Processing Excel sheet", sheet_name=sheet_name)
                
                # Look for URL-like content in all columns
                for column in sheet_df.columns:
                    for value in sheet_df[column].dropna():
                        str_value = str(value).strip()
                        if str_value and ('http' in str_value or '.' in str_value):
                            urls.append(str_value)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_urls = []
            for url in urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            
            return unique_urls
            
        except Exception as e:
            logger.error("Failed to process Excel file", error=str(e))
            raise ValueError(f"Failed to process Excel file: {str(e)}")
    
    async def validate_and_clean_urls(self, urls: List[str]) -> List[str]:
        """Validate and clean extracted URLs."""
        logger.debug("Validating URLs", count=len(urls))
        
        valid_urls = []
        validation_stats = {
            "total": len(urls),
            "valid": 0,
            "invalid": 0,
            "duplicates": 0
        }
        
        seen_urls = set()
        
        for url in urls:
            try:
                # Clean whitespace
                cleaned_url = url.strip()
                
                # Skip empty URLs
                if not cleaned_url:
                    continue
                
                # Skip comments and non-URL lines
                if cleaned_url.startswith('#') or cleaned_url.startswith('//'):
                    continue
                
                # Normalize URL
                try:
                    normalized_url = URLValidator.normalize_url(cleaned_url)
                except Exception:
                    validation_stats["invalid"] += 1
                    continue
                
                # Validate URL
                if not URLValidator.is_valid_url(normalized_url):
                    validation_stats["invalid"] += 1
                    continue
                
                # Check for duplicates
                if normalized_url in seen_urls:
                    validation_stats["duplicates"] += 1
                    continue
                
                seen_urls.add(normalized_url)
                valid_urls.append(normalized_url)
                validation_stats["valid"] += 1
                
            except Exception as e:
                logger.debug("URL validation error", url=url, error=str(e))
                validation_stats["invalid"] += 1
                continue
        
        logger.info("URL validation completed", stats=validation_stats)
        
        return valid_urls
    
    async def analyze_file_structure(self, content: bytes, file_path: str) -> Dict[str, Any]:
        """Analyze file structure and provide metadata."""
        file_extension = Path(file_path).suffix.lower()
        analysis = {
            "file_type": file_extension,
            "file_size": len(content),
            "encoding": "utf-8",
            "structure": {}
        }
        
        try:
            if file_extension == '.csv':
                text_content = content.decode('utf-8', errors='ignore')
                csv_file = io.StringIO(text_content)
                reader = csv.reader(csv_file)
                
                # Analyze CSV structure
                rows = list(reader)
                analysis["structure"] = {
                    "total_rows": len(rows),
                    "columns": len(rows[0]) if rows else 0,
                    "has_header": csv.Sniffer().has_header(text_content[:1024]) if rows else False
                }
            
            elif file_extension == '.json':
                text_content = content.decode('utf-8', errors='ignore')
                data = json.loads(text_content)
                
                analysis["structure"] = {
                    "type": type(data).__name__,
                    "size": len(data) if isinstance(data, (list, dict)) else 1
                }
            
            elif file_extension in ['.xlsx', '.xls']:
                excel_file = io.BytesIO(content)
                df_dict = pd.read_excel(excel_file, sheet_name=None)
                
                analysis["structure"] = {
                    "sheets": list(df_dict.keys()),
                    "total_sheets": len(df_dict),
                    "total_rows": sum(len(df) for df in df_dict.values()),
                    "total_columns": sum(len(df.columns) for df in df_dict.values())
                }
        
        except Exception as e:
            logger.warning("File structure analysis failed", file_path=file_path, error=str(e))
            analysis["analysis_error"] = str(e)
        
        return analysis