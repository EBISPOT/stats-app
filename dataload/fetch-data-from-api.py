import yaml
import json
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import os
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import urllib3
from typing import Dict, List, Optional

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description='Fetch logs from Elasticsearch')
    parser.add_argument('days', type=int, nargs='?', default=1,
                       help='Number of days to look back for data (default: 1)')
    return parser.parse_args()

class ConfigurationError(Exception):
    """Raised when there's an error in configuration"""
    pass

class DataIngestionService:
    def __init__(self):
        self._load_environment()
        self.session = requests.Session()
        self.config = None
        self.total_logs_per_resource = {}
        
    def _load_environment(self):
        script_dir = Path(__file__).resolve().parent
        root_dir = script_dir.parent
        env_path = root_dir / '.env'
        
        if not env_path.exists():
            raise ConfigurationError(f"Environment file not found at {env_path}")
        
        load_dotenv(env_path)
        
        # Set up configuration
        self.es_base_url = os.getenv('ES_HOST')
        # FTP logs have a different base URL
        self.ftp_base_url = os.getenv('FTP_ES_HOST')
        self.es_user = os.getenv('ES_USER')
        self.es_password = os.getenv('ES_PASSWORD')
        self.output_dir = os.getenv('STAGING_AREA_PATH')
        
        # Check for required environment variables
        required_vars = {
            'ES_HOST': self.es_base_url,
            'ES_USER': self.es_user,
            'ES_PASSWORD': self.es_password,
            'STAGING_AREA_PATH': self.output_dir
        }
    
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ConfigurationError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        self.config_path = script_dir.parent / os.getenv('CONFIG_FILE', 'config.yaml')
        
        # Construct the search URLs
        self.web_search_url = f"{self.es_base_url}/live*/_search"
        self.ftp_search_url = f"{self.ftp_base_url}/ftplogs*/_search"
        
    def initialize(self) -> bool:
        """Initialize service and test connection"""
        try:
            # Load YAML config
            if not self.config_path.exists():
                raise ConfigurationError(f"Config file not found at {self.config_path}")
            
            with open(self.config_path) as f:
                self.config = yaml.safe_load(f)
            
            # Test connection with a simple search
            test_query = {
                "size": 1,
                "query": {"match_all": {}}
            }
            
            response = requests.post(
                self.web_search_url,
                auth=HTTPBasicAuth(self.es_user, self.es_password),
                headers={"Content-Type": "application/json"},
                json=test_query,
                verify=False
            )
            response.raise_for_status()
            logger.info("Successfully connected to Elasticsearch")
            
            # Create output directory if it doesn't exist
            self._create_directory_structure()
            
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            return False
    
    def _create_directory_structure(self):
        """Create the directory structure for storing logs"""
        today = datetime.now()
        year_month_day = today.strftime("%Y/%m/%d")
        full_path = Path(self.output_dir) / year_month_day
        full_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory structure: {full_path}")

    def _build_query(self, destination_host: str, pattern: str, start_time: datetime, end_time: datetime, is_ftp: bool, search_after: Optional[List] = None) -> Dict:
        query = {
            "size": 5000,
            "sort": [
                {"@timestamp": {"order": "desc"}},
                {"_id": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_time.isoformat(),
                                    "lte": end_time.isoformat()
                                }
                            }
                        }
                    ]
                }
            }
        }
    
        # Add conditional parts based on is_ftp
        if is_ftp:
            # FTP query - use file_name only, ignore destination_host
            query["query"]["bool"]["must"].append({
                "match_phrase_prefix": {
                    "file_name": pattern
                }
            })
        elif pattern == "/*":
            # If pattern is "/*", we just want the destination host (in case of cancermodels and immunophenotype)
            query["query"]["bool"]["must"].append({
                "match": {
                    "destination.address": destination_host
                }
            })
        else:
            # HTTP query - use request_uri_path and destination_host
            query["query"]["bool"]["must"].extend([
                {
                    "match": {
                        "destination.address": destination_host
                    }                    
                },
                {
                    "match": {
                        "url.path": pattern
                    }
                }
                ])        
        
        if search_after:
            query["search_after"] = search_after
        return query

    def fetch_logs(self, days_back: int = 1) -> bool:
        """Fetch logs from Elasticsearch"""
        if not self.config:
            logger.error("Service not initialized. Call initialize() first.")
            return False
        
        end_time = datetime.now().astimezone()
        start_time = end_time - timedelta(days=days_back)

        logger.info(f"Fetching logs from {start_time} to {end_time} ({days_back} day(s))")
        
        try:
            for resource in self.config.get('resources', []):
                resource_name = resource.get('name')
                endpoints = resource.get('endpoints', [])
                destination_host = resource.get('destination-host')
                
                logger.info(f"Processing resource: {resource_name}")
                
                resource_total = 0
                all_logs = []
                for endpoint in endpoints:
                    total_hits = 0
                    from_size = 0
                    search_after = None

                    is_ftp = endpoint.startswith("/pub/databases/")
                    search_url = self.ftp_search_url if is_ftp else self.web_search_url
                    logger.info(f"Using {search_url} for {'FTP' if is_ftp else 'web'} endpoint: {endpoint}")

                    while True:
                        query = self._build_query(destination_host, endpoint, start_time, end_time, is_ftp, search_after)
                        logger.debug(f"Query for {endpoint}: {json.dumps(query, indent=2)}")
                    
                        try:
                            response = requests.post(
                                search_url,
                                auth=HTTPBasicAuth(self.es_user, self.es_password),
                                headers={"Content-Type": "application/json"},
                                json=query,
                                verify=False
                            )
                        
                            response.raise_for_status()
                            data = response.json()
                            hits = data.get('hits', {}).get('hits', [])

                            if not hits:
                                break
                            
                            batch_size = len(hits)
                            all_logs.extend(hits)
                            total_hits += batch_size
                            resource_total += batch_size
                            from_size += batch_size

                            logger.info(f"Fetched {batch_size} hits for endpoint: {endpoint}. "
                                      f"Total for endpoint: {total_hits}, "
                                      f"Total for resource: {resource_total}")

                            last_hit = hits[-1]
                            search_after = [last_hit['sort'][0], last_hit['sort'][1]]
                            
                            logger.info(f"Fetched {len(hits)} hits for endpoint: {endpoint}. Total so far: {total_hits}")

                            if len(all_logs) >= 50000:
                                self.total_logs_per_resource[resource_name] = resource_total
                                self._save_logs(all_logs, resource_name, start_time, is_intermediate=True)
                                all_logs = []  # Reset to free memory
                        
                        
                        except Exception as e:
                            logger.error(f"Error fetching logs for endpoint {endpoint}: {str(e)}")
                            continue

                    logger.info(f"Completed fetching {total_hits} logs for endpoint: {endpoint}")
                
                if all_logs:
                    self.total_logs_per_resource[resource_name] = resource_total
                    self._save_logs(all_logs, resource_name, start_time, is_intermediate=True)
                else:
                    logger.warning(f"No logs found for resource: {resource_name}")
                

                if resource_total > 0:
                    logger.info(f"Total logs fetched for {resource_name}: {resource_total}")
                else:
                    logger.warning(f"No logs found for resource: {resource_name}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error in fetch_logs: {str(e)}")
            return False

    def _save_logs(self, logs: List[Dict], resource_name: str, timestamp: datetime, is_intermediate: bool = True) -> None:
        """Save logs to filesystem"""
        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        
        # All files go under the resource directory
        resource_path = Path(self.output_dir) / year / month / day / resource_name
        
        if is_intermediate:
            # For intermediate files, include count in filename
            current_count = self.total_logs_per_resource.get(resource_name, 0)
            filename = f"{resource_name}_intermediate_{current_count}.json"
        else:
            # Final consolidated file
            filename = f"{resource_name}.json"
        
        full_path = resource_path / filename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(full_path, 'w') as f:
            json.dump(logs, f, indent=2)
        
        logger.info(f"Saved {len(logs)} logs to: {full_path}")

def main():
    if os.getenv('DEBUG', '').lower() in ('true', '1', 'yes'):
        logging.getLogger().setLevel(logging.DEBUG)
    try:
        args = parse_args()
        service = DataIngestionService()
        if service.initialize():
            if service.fetch_logs(days_back=args.days):
                logger.info(f"Log ingestion completed successfully for the last {args.days} day(s)")
                return 0
            else:
                logger.error("Log ingestion failed")
                return 1
        else:
            logger.error("Service initialization failed")
            return 1
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())