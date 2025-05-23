import json
import logging
from datetime import datetime
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs
import psycopg2
from psycopg2.extras import execute_values
from dataclasses import dataclass
from collections import defaultdict
import os
from dotenv import load_dotenv

# Load .env from root directory
root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

def get_db_config():
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST')
    }

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class RequestData:
    """Data class to hold processed request information"""
    endpoint: str
    request_date: datetime
    country: Optional[str]
    parameters: Dict[str, str]

class DatabaseLoader:
    def __init__(self, db_config: Dict[str, str], staging_dir: Path):
        """Initialize database connection and prepare lookup caches"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.staging_dir = staging_dir
        self.processed_dir = staging_dir.parent / 'processed-logs'
        # In-memory caches for lookup tables
        self.resource_cache = {}  # name -> id
        self.endpoint_cache = {}  # (path, resource_id) -> id
        self.country_cache = {}   # name -> id
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _get_or_create_resource(self, resource_name: str) -> int:
        """Get resource ID from cache or create new resource"""
        if resource_name not in self.resource_cache:
            self.cursor.execute(
                """
                INSERT INTO resources (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (resource_name,)
            )
            self.resource_cache[resource_name] = self.cursor.fetchone()[0]
        return self.resource_cache[resource_name]

    def _get_or_create_endpoint(self, path: str, resource_id: int) -> int:
        """Get endpoint ID from cache or create new endpoint"""
        cache_key = (path, resource_id)
        if cache_key not in self.endpoint_cache:
            self.cursor.execute(
                """
                INSERT INTO endpoints (path, resource_id)
                VALUES (%s, %s)
                ON CONFLICT (path, resource_id) DO UPDATE SET path = EXCLUDED.path
                RETURNING id
                """,
                (path, resource_id)
            )
            self.endpoint_cache[cache_key] = self.cursor.fetchone()[0]
        return self.endpoint_cache[cache_key]

    def _get_or_create_country(self, country_name: str) -> Optional[int]:
        """Get country ID from cache or create new country"""
        if not country_name:
            return None
            
        if country_name not in self.country_cache:
            self.cursor.execute(
                """
                INSERT INTO countries (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (country_name,)
            )
            self.country_cache[country_name] = self.cursor.fetchone()[0]
        return self.country_cache[country_name]

    def _process_log_entry(self, entry: Dict) -> RequestData:
        """Process a single log entry and extract relevant information"""
        source = entry.get('_source', {})
        
        # Extract endpoint
        endpoint = source.get('request_uri_path', '')
        request_params = source.get('request_query', '')
        # Parse URL parameters (only if '?' exists in endpoint)
        parameters = {}
        if '?' in request_params:
            base_url, query = request_params.split('?', 1)
            parameters = parse_qs(query)
            # Flatten parameters (take first value if multiple exist)
            parameters = {k: v[0] if isinstance(v, list) else v 
                        for k, v in parameters.items()}
        
        # Extract timestamp and country
        timestamp = datetime.fromisoformat(source.get('@timestamp').replace('Z', '+00:00'))
        country = source.get('geoip', {}).get('country_name')
        
        return RequestData(
            endpoint=endpoint,
            request_date=timestamp,
            country=country,
            parameters=parameters
        )

    def _process_batch(self, requests_batch: List[tuple], parameters_batch: List[Dict]):
        """Process a batch of requests and their parameters"""
        try:
            # Insert requests using execute_values correctly
            sql = """
                INSERT INTO requests (request_date, resource_id, endpoint_id, request_timestamp, country_id)
                VALUES %s
                ON CONFLICT (request_date, request_timestamp, endpoint_id, resource_id) DO NOTHING
                RETURNING id, endpoint_id
            """
            # Convert batch to list of tuples for execute_values
            execute_values(
                self.cursor,
                sql,
                list(requests_batch),
                template='(%s, %s, %s, %s, %s)'
            )

            # Get the inserted request IDs
            request_ids = self.cursor.fetchall()

            # Create a mapping of endpoint_id to request_id for parameters
            endpoint_to_request = {endpoint_id: request_id for request_id, endpoint_id in request_ids}

            # Update parameters with actual request IDs and insert
            if parameters_batch and request_ids:
                param_values = [
                    (endpoint_to_request[param['endpoint_id']],
                     param['request_date'],
                     param['param_name'],
                     param['param_value'])
                    for param in parameters_batch
                    if param['endpoint_id'] in endpoint_to_request
                ]

                execute_values(
                    self.cursor,
                    """
                    INSERT INTO parameters (request_id, request_date, param_name, param_value)
                    VALUES %s
                    """,
                    param_values,
                    template='(%s, %s, %s, %s)'
                )
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            raise

    def process_file(self, file_path: Path, resource_name: str):
        """Process a single JSON file of logs"""
        logger.info(f"Processing file: {file_path} for resource: {resource_name}")

        try:
            # Read and parse JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Handle both list and dictionary inputs
            logs = data if isinstance(data, list) else [data]

            # Get or create resource ID
            resource_id = self._get_or_create_resource(resource_name)

            # Process logs in batches
            batch_size = 1000
            requests_batch = []
            parameters_batch = []

            for log in logs:
                try:
                    request_data = self._process_log_entry(log)

                    # Get or create related IDs
                    endpoint_id = self._get_or_create_endpoint(request_data.endpoint, resource_id)
                    country_id = self._get_or_create_country(request_data.country)

                    # Add request to batch
                    requests_batch.append((
                        request_data.request_date.date(),  # store date for partitioning
                        resource_id,
                        endpoint_id,
                        request_data.request_date,  # store full timestamp
                        country_id
                    ))

                    # If we have parameters, prepare them for batch insert
                    if request_data.parameters:
                        for param_name, param_value in request_data.parameters.items():
                            parameters_batch.append({
                                'request_date': request_data.request_date.date(),
                                'param_name': param_name,
                                'param_value': param_value,
                                'endpoint_id': endpoint_id  # Temporary storage for lookup
                            })

                    # Process batch if it reaches the size limit
                    if len(requests_batch) >= batch_size:
                        self._process_batch(requests_batch, parameters_batch)
                        requests_batch = []
                        parameters_batch = []

                except Exception as e:
                    logger.error(f"Error processing log entry: {str(e)}")
                    continue

            # Process any remaining records
            if requests_batch:
                self._process_batch(requests_batch, parameters_batch)

            # After successful processing, move to processed directory
            relative_path = file_path.relative_to(self.staging_dir)
            
            # Create the processed file path
            processed_path = self.processed_dir / relative_path
            
            # Create processed directory structure if it doesn't exist
            processed_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Move the file
            shutil.move(str(file_path), str(processed_path))
            logger.info(f"Moved processed file to: {processed_path}")

            self.conn.commit()
            logger.info(f"Successfully processed file: {file_path}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

def process_staging_area(loader: DatabaseLoader):
    """Process the staging area with year/month/day/resource directory structure"""
    staging_dir = loader.staging_dir
    # Iterate through year directories
    for year_dir in staging_dir.glob('*'):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
            
        # Iterate through month directories
        for month_dir in year_dir.glob('*'):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
                
            # Iterate through day directories
            for day_dir in month_dir.glob('*'):
                if not day_dir.is_dir() or not day_dir.name.isdigit():
                    continue
                
                logger.info(f"Processing data for {year_dir.name}-{month_dir.name}-{day_dir.name}")
                
                # Iterate through resource directories
                for resource_dir in day_dir.glob('*'):
                    if not resource_dir.is_dir():
                        continue
                        
                    resource_name = resource_dir.name
                    logger.info(f"Processing resource: {resource_name}")
                    
                    # Process all JSON files in the resource directory
                    for json_file in resource_dir.glob('*.json'):
                        try:
                            loader.process_file(json_file, resource_name)
                        except Exception as e:
                            logger.error(f"Failed to process file {json_file}: {str(e)}")
                            continue

def main():
    # Get database configuration
    db_config = get_db_config()

    # Directory containing staged files
    staging_dir = Path(os.getenv('STAGING_AREA_PATH'))
    logger.info(f"Looking for resources in: {staging_dir}")
    loader = DatabaseLoader(db_config, staging_dir)
    
    try:
        loader.connect()
        
        # Process all resource directories
        process_staging_area(loader)
                    
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        loader.close()

if __name__ == "__main__":
    main()