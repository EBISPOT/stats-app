import json
import logging
from datetime import datetime
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set
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
    
@dataclass
class ProcessedRequest:
    """Data class for request with resolved IDs"""
    request_date_partition: datetime  # date for partitioning
    resource_id: int
    endpoint_id: int
    request_timestamp: datetime  # full timestamp
    country_id: Optional[int]
    parameters: Dict[str, str]

class DatabaseLoader:
    def __init__(self, db_config: Dict[str, str], staging_dir: Path):
        """Initialize database connection and prepare lookup caches"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.staging_dir = staging_dir
        self.processed_dir = staging_dir.parent / 'processed-logs'
        # Small caches for resources and countries (these don't grow much)
        self.resource_cache = {}  # name -> id
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
            
    def _populate_caches(self):
        """Pre-load resources and countries into memory"""
        try:
            # Load resources
            self.cursor.execute("SELECT id, name FROM resources")
            self.resource_cache = {name: id for id, name in self.cursor.fetchall()}
            logger.info(f"Loaded {len(self.resource_cache)} resources into cache")
            
            # Load countries
            self.cursor.execute("SELECT id, name FROM countries")
            self.country_cache = {name: id for id, name in self.cursor.fetchall()}
            logger.info(f"Loaded {len(self.country_cache)} countries into cache")
        except Exception as e:
            logger.error(f"Failed to populate caches: {e}")
            raise

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
    
    def _batch_get_or_create_endpoints(self, endpoints: Set[str], resource_id: int) -> Dict[str, int]:
        """Batch process endpoints and return mapping"""
        if not endpoints:
            return {}
        
        # Insert all endpoints (existing ones will be ignored)
        values = [(path, resource_id) for path in endpoints]
        execute_values(
            self.cursor,
            """
            INSERT INTO endpoints (path, resource_id) 
            VALUES %s
            ON CONFLICT (path, resource_id) DO NOTHING
            """,
            values
        )
        
        # Now fetch all endpoints we need
        self.cursor.execute(
            """
            SELECT id, path 
            FROM endpoints 
            WHERE resource_id = %s AND path = ANY(%s)
            """,
            (resource_id, list(endpoints))
        )
        
        # Build mapping
        endpoint_mapping = {path: id for id, path in self.cursor.fetchall()}
        
        # Simple debug check
        if len(endpoint_mapping) != len(endpoints):
            missing = endpoints - set(endpoint_mapping.keys())
            logger.error(f"Failed to map {len(missing)} endpoints: {list(missing)[:5]}")
        
        return endpoint_mapping
    
    def _batch_get_or_create_countries(self, countries: Set[str]) -> Dict[str, int]:
        """Batch process countries and return mapping"""
        # Filter out None values and already cached countries
        new_countries = {c for c in countries if c and c not in self.country_cache}
        
        if new_countries:
            # Insert all new countries (existing ones will be ignored)
            execute_values(
                self.cursor,
                """
                INSERT INTO countries (name) 
                VALUES %s
                ON CONFLICT (name) DO NOTHING
                """,
                [(c,) for c in new_countries]
            )
            
            # Fetch all new countries
            self.cursor.execute(
                """
                SELECT id, name 
                FROM countries 
                WHERE name = ANY(%s)
                """,
                (list(new_countries),)
            )
            
            # Update cache with new countries
            for id, name in self.cursor.fetchall():
                self.country_cache[name] = id
        
        # Return mapping including cached values
        return {c: self.country_cache[c] for c in countries if c}
    
    def _process_log_entry(self, entry: Dict) -> RequestData:
        """Process a single log entry and extract relevant information"""
        source = entry.get('_source', {})
        
        # Extract endpoint
        endpoint = source.get('url.path', '')
        # Parse URL parameters (only if '?' exists in endpoint)
        parameters = {}
        if '?' in endpoint:
            base_url, query = endpoint.split('?', 1)
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
        
    def _batch_insert_requests_and_parameters(self, requests: List[ProcessedRequest]):
        """Batch insert requests and their parameters"""
        if not requests:
            return
            
        # Prepare request data for batch insert
        request_values = [
            (
                r.request_date_partition,
                r.resource_id,
                r.endpoint_id,
                r.request_timestamp,
                r.country_id
            )
            for r in requests
        ]
        
        # Insert requests - let database handle duplicates
        execute_values(
            self.cursor,
            """
            INSERT INTO requests (request_date, resource_id, endpoint_id, request_timestamp, country_id)
            VALUES %s
            ON CONFLICT (request_date, request_timestamp, endpoint_id, resource_id) DO NOTHING
            RETURNING id, endpoint_id, request_timestamp
            """,
            request_values,
            template='(%s, %s, %s, %s, %s)'
        )
        
        # Get inserted request IDs
        inserted_requests = self.cursor.fetchall()
        
        if not inserted_requests:
            logger.warning("No new requests inserted (all duplicates)")
            return
            
        # For parameters, we need to match them correctly
        # Since we might have multiple requests with same endpoint,
        # we'll insert parameters for ALL successfully inserted requests
        
        # Prepare parameter data - only for requests that were actually inserted
        parameter_values = []
        for request_id, endpoint_id, timestamp in inserted_requests:
            # Find the matching request from our original list
            for r in requests:
                if r.endpoint_id == endpoint_id and r.request_timestamp == timestamp and r.parameters:
                    for param_name, param_value in r.parameters.items():
                        parameter_values.append((
                            request_id,
                            r.request_date_partition,
                            param_name,
                            str(param_value)
                        ))
                    break  # Found the matching request, move to next
        
        # Batch insert parameters
        if parameter_values:
            try:
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO parameters (request_id, request_date, param_name, param_value)
                    VALUES %s
                    """,
                    parameter_values,
                    template='(%s, %s, %s, %s)'
                )
                logger.debug(f"Successfully inserted {len(parameter_values)} parameters")
            except psycopg2.errors.ForeignKeyViolation as e:
                logger.error(f"Foreign key violation in parameters: {e}")
                logger.error(f"This should not happen - please check request_mapping logic")
                raise
        
    def process_file(self, file_path: Path, resource_name: str):
        """Process a single JSON file of logs using batch processing"""
        logger.info(f"Processing file: {file_path} for resource: {resource_name}")
        failed_records = []
        
        try:
            # Read and parse JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Handle both list and dictionary inputs
            logs = data if isinstance(data, list) else [data]
            total_records = len(logs)
            
            # Get or create resource ID
            resource_id = self._get_or_create_resource(resource_name)

            # First pass: collect all unique values and parse requests
            unique_endpoints = set()
            unique_countries = set()
            parsed_requests = []
            
            for idx, log in enumerate(logs):
                try:
                    request_data = self._process_log_entry(log)
                    unique_endpoints.add(request_data.endpoint)
                    if request_data.country:
                        unique_countries.add(request_data.country)
                    parsed_requests.append(request_data)
                except Exception as e:
                    logger.error(f"Error processing log entry {idx}: {str(e)}")
                    failed_records.append(idx)
                    continue

            # Log statistics
            logger.info(f"Found {len(unique_endpoints)} unique endpoints and "
                       f"{len(unique_countries)} unique countries in {total_records} records")

            # Batch process all lookups
            endpoint_mapping = self._batch_get_or_create_endpoints(unique_endpoints, resource_id)
            country_mapping = self._batch_get_or_create_countries(unique_countries)

            # Second pass: create processed requests with resolved IDs
            processed_requests = []
            for request_data in parsed_requests:
                endpoint_id = endpoint_mapping.get(request_data.endpoint)
                if not endpoint_id:
                    logger.error(f"Failed to get endpoint ID for: {request_data.endpoint}")
                    continue
                    
                country_id = country_mapping.get(request_data.country) if request_data.country else None
                
                processed_requests.append(ProcessedRequest(
                    request_date_partition=request_data.request_date.date(),
                    resource_id=resource_id,
                    endpoint_id=endpoint_id,
                    request_timestamp=request_data.request_date,
                    country_id=country_id,
                    parameters=request_data.parameters
                ))

            # Batch insert all requests and parameters
            batch_size = 5000  # Larger batch size since we're doing fewer operations
            for i in range(0, len(processed_requests), batch_size):
                batch = processed_requests[i:i + batch_size]
                self._batch_insert_requests_and_parameters(batch)
                logger.info(f"Inserted batch {i//batch_size + 1} of {len(processed_requests)//batch_size + 1}")

            # Commit the transaction
            self.conn.commit()
            
            # Move file to processed directory only if successfully processed
            relative_path = file_path.relative_to(self.staging_dir)
            processed_path = self.processed_dir / relative_path
            processed_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file_path), str(processed_path))
            logger.info(f"Successfully processed {len(processed_requests)}/{total_records} records. "
                       f"Moved file to: {processed_path}")

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
        loader._populate_caches()
        
        # Process all resource directories
        process_staging_area(loader)
                    
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        loader.close()

if __name__ == "__main__":
    main()