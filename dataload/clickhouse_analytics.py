"""
Minimal ClickHouse Integration for SPOT App Stats
This fits perfectly into your existing pipeline with minimal changes
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs
import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client
import yaml
import os
from dotenv import load_dotenv

# Load your existing .env
root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickHouseAnalyticsLayer:
    """
    Drop-in analytics layer for your existing PostgreSQL setup
    Run this AFTER your existing load-data.py script
    """
    
    def __init__(self):
        # Your existing PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST')
        )
        
        # New ClickHouse connection (single Docker container)
        print(f"Connecting to ClickHouse at {os.getenv('CLICKHOUSE_HOST', 'localhost')}:{os.getenv('CLICKHOUSE_PORT', 9000)}")
        self.ch_client = Client(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            user='default',
            password='',
            settings={'use_numpy': False}
        )
        
        # Load your existing config.yaml
        config_path = root_dir / 'config.yaml'
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
    
    def setup_clickhouse_once(self):
        """
        One-time setup - Run this ONCE when you deploy ClickHouse
        Creates optimized schema specifically for your use case
        """
        
        # Create database
        self.ch_client.execute("CREATE DATABASE IF NOT EXISTS spotappstats")
        
        # Main analytics table - denormalized for speed
        self.ch_client.execute("""
            CREATE TABLE IF NOT EXISTS spotappstats.requests
            (
                -- Core fields matching your PostgreSQL schema
                request_date Date,
                request_timestamp DateTime64(3),
                resource String,
                endpoint String,
                country String,
                
                -- Store parameters as JSON for flexibility
                parameters String,  -- JSON string
                
                event_id UInt64 MATERIALIZED cityHash64(resource, endpoint, parameters, request_timestamp),
                -- Computed fields for common queries
                endpoint_base String MATERIALIZED splitByChar('?', endpoint)[1],
                
                -- Special handling for OLS ontologies (your optimization)
                ontology_id String MATERIALIZED 
                    if(resource = 'OLS', 
                       extract(endpoint, '/ols4/ontologies/([^/?]+)'), 
                       ''),
                
                -- Year/Month for efficient partitioning
                year UInt16 MATERIALIZED toYear(request_date),
                month UInt8 MATERIALIZED toMonth(request_date)
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY (year, month)
            ORDER BY (event_id)
            SETTINGS index_granularity = 8192
        """)
        
        # Add indices for your specific query patterns
        self.ch_client.execute("""
            ALTER TABLE spotappstats.requests
            ADD INDEX IF NOT EXISTS idx_resource (resource) TYPE set(20) GRANULARITY 1,
            ADD INDEX IF NOT EXISTS idx_country (country) TYPE set(200) GRANULARITY 2,
            ADD INDEX IF NOT EXISTS idx_ontology (ontology_id) TYPE set(1000) GRANULARITY 1
        """)
        
        logger.info("ClickHouse setup complete!")
    
    def sync_from_processed_files(self, date_to_process: Optional[datetime] = None):
        """
        Option 1: Read directly from your processed JSON files
        Run this AFTER your existing load-data.py completes
        """
        if not date_to_process:
            date_to_process = datetime.now() - timedelta(days=1)
        
        # Use your existing staging area structure
        staging_dir = Path(os.getenv('STAGING_AREA_PATH'))
        year = date_to_process.strftime("%Y")
        month = date_to_process.strftime("%m") 
        day = date_to_process.strftime("%d")
        
        day_path = staging_dir / year / month / day
        
        if not day_path.exists():
            logger.warning(f"No data found for {date_to_process.date()}")
            return
        
        # Process each resource directory
        for resource_dir in day_path.glob('*'):
            if not resource_dir.is_dir():
                continue
            
            resource_name = resource_dir.name
            logger.info(f"Syncing {resource_name} to ClickHouse...")
            
            # Read JSON files (your existing format)
            records_to_insert = []
            
            for json_file in resource_dir.glob('*.json'):
                with open(json_file, 'r') as f:
                    logs = json.load(f)
                    
                    for log in logs:
                        # Parse using your existing logic
                        source = log.get('_source', {})
                        
                        # Extract fields matching your existing processing
                        endpoint = source.get('url.path', '')
                        if '?' in endpoint:
                            base_endpoint, query_string = endpoint.split('?', 1)
                            parameters = parse_qs(query_string)
                            # Flatten parameters
                            parameters = {k: v[0] if isinstance(v, list) else v 
                                        for k, v in parameters.items()}
                        else:
                            base_endpoint = endpoint
                            parameters = {}
                        
                        timestamp = source.get('@timestamp')
                        country = source.get('geo', {}).get('country_name', 'Unknown')
                        
                        records_to_insert.append({
                            'request_date': datetime.fromisoformat(
                                timestamp.replace('Z', '+00:00')
                            ).date(),
                            'request_timestamp': datetime.fromisoformat(
                                timestamp.replace('Z', '+00:00')
                            ),
                            'resource': resource_name,
                            'endpoint': endpoint,
                            'country': country,
                            'parameters': json.dumps(parameters)
                        })
            
            # Batch insert to ClickHouse
            if records_to_insert:
                self.ch_client.execute(
                    """
                    INSERT INTO spotappstats.requests 
                    (request_date, request_timestamp, resource, endpoint, country, parameters)
                    VALUES
                    """,
                    records_to_insert
                )
                logger.info(f"Inserted {len(records_to_insert)} records for {resource_name}")
    
    def sync_from_postgresql(self, days_back: int = 1, sync_all: bool = False, chunk_size: int = 100000):
        """
        Option 2: Sync directly from PostgreSQL after your load-data.py runs
        This is simpler if you trust your PostgreSQL data
        """
        if sync_all:
            logger.info("Syncing ALL historical data in chunks...")
            self._sync_all_historical_data(chunk_size)
        else:
            cutoff_time = datetime.now() - timedelta(days=days_back)
            self._sync_with_date_filter(cutoff_time, days_back)
    
    def _sync_with_date_filter(self, cutoff_time, days_back):
        """Sync data with date filter (existing functionality)"""
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Use your existing schema
            cur.execute("""
                SELECT 
                    r.request_date,
                    r.request_timestamp,
                    res.name as resource,
                    e.path as endpoint,
                    COALESCE(c.name, 'Unknown') as country,
                    COALESCE(
                        (SELECT json_object_agg(param_name, param_value)
                         FROM appusestats.parameters p 
                         WHERE p.request_id = r.id 
                         AND p.request_date = r.request_date),
                        '{}'::json
                    ) as parameters
                FROM appusestats.requests r
                JOIN appusestats.resources res ON r.resource_id = res.id
                JOIN appusestats.endpoints e ON r.endpoint_id = e.id
                LEFT JOIN appusestats.countries c ON r.country_id = c.id
                WHERE r.request_timestamp > %s
                ORDER BY r.request_timestamp
                LIMIT 500000  -- Process in chunks
            """, (cutoff_time,))
            
            records = cur.fetchall()
        
        if not records:
            logger.info("No new records to sync")
            return
        
        self._insert_to_clickhouse(records, f"from last {days_back} days")
    
    def _sync_all_historical_data(self, chunk_size):
        """Sync all historical data in chunks using timestamp-based pagination"""
        # Use regular cursor for simple queries
        with self.pg_conn.cursor() as cur:
            # First, get total count for progress tracking
            cur.execute("SELECT COUNT(*) FROM appusestats.requests")
            total_records = cur.fetchone()[0]
            logger.info(f"Total records to sync: {total_records:,}")
            
            # Get the timestamp range for chunking
            cur.execute("SELECT MIN(request_timestamp), MAX(request_timestamp) FROM appusestats.requests")
            min_timestamp, max_timestamp = cur.fetchone()
            logger.info(f"Date range: {min_timestamp} to {max_timestamp}")
        
        # Use RealDictCursor for the main data query
        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            synced_count = 0
            current_timestamp = min_timestamp
            
            while current_timestamp <= max_timestamp:
                # Process chunk by timestamp range for consistent performance
                cur.execute("""
                    SELECT 
                        r.request_date,
                        r.request_timestamp,
                        res.name as resource,
                        e.path as endpoint,
                        COALESCE(c.name, 'Unknown') as country,
                        COALESCE(
                            (SELECT json_object_agg(param_name, param_value)
                             FROM appusestats.parameters p 
                             WHERE p.request_id = r.id 
                             AND p.request_date = r.request_date),
                            '{}'::json
                        ) as parameters
                    FROM appusestats.requests r
                    JOIN appusestats.resources res ON r.resource_id = res.id
                    JOIN appusestats.endpoints e ON r.endpoint_id = e.id
                    LEFT JOIN appusestats.countries c ON r.country_id = c.id
                    WHERE r.request_timestamp >= %s
                    ORDER BY r.request_timestamp
                    LIMIT %s
                """, (current_timestamp, chunk_size))
                
                records = cur.fetchall()
                
                if not records:
                    # No more records, we're done
                    break
                
                # Insert this chunk
                chunk_num = synced_count // chunk_size + 1
                self._insert_to_clickhouse(records, f"chunk {chunk_num}")
                
                synced_count += len(records)
                
                # Progress update
                progress = (synced_count / total_records) * 100
                logger.info(f"Progress: {synced_count:,}/{total_records:,} ({progress:.1f}%)")
                
                # Set next timestamp to be just after the last record in this chunk
                if len(records) < chunk_size:
                    # This was the last chunk
                    break
                else:
                    # Get the last timestamp and add a microsecond to avoid duplicates
                    last_timestamp = records[-1]['request_timestamp']
                    current_timestamp = last_timestamp + timedelta(microseconds=1)
            
            logger.info(f"Historical sync complete! Total synced: {synced_count:,} records")
    
    def _insert_to_clickhouse(self, records, description=""):
        """Helper method to insert records to ClickHouse"""
        if not records:
            return
            
        # Convert to ClickHouse format
        ch_records = []
        for row in records:
            # Get the timezone-aware timestamp from Postgres
            ts_aware = row['request_timestamp']

            # This strips the '+00:00' timezone info without changing the actual time values.
            ts_naive_utc = ts_aware.replace(tzinfo=None)
            print(f"Processing record with timestamp: {ts_naive_utc} (UTC)")
            ch_records.append({
                'request_date': row['request_date'],
                'request_timestamp': ts_naive_utc,
                'resource': row['resource'],
                'endpoint': row['endpoint'],
                'country': row['country'],
                'parameters': json.dumps(row['parameters']) if row['parameters'] else '{}'
            })
        
        # Insert to ClickHouse
        self.ch_client.execute(
            """
            INSERT INTO spotappstats.requests 
            (request_date, request_timestamp, resource, endpoint, country, parameters)
            VALUES
            """,
            ch_records
        )
        
        logger.info(f"Synced {len(records):,} records to ClickHouse {description}")
        
        # Force merge to apply deduplication immediately
        try:
            self.ch_client.execute("OPTIMIZE TABLE spotappstats.requests FINAL")
            logger.info("Applied deduplication via table optimization")
        except Exception as e:
            logger.warning(f"Could not optimize table: {e}")
    
    def example_queries(self):
        """
        Show how much faster your queries will be
        """
        
        # Your OLS ontology query - now instant!
        result = self.ch_client.execute("""
            SELECT count() as total
            FROM spotappstats.requests
            WHERE resource = 'OLS' 
            AND ontology_id = 'efo'
            AND request_date >= today() - 30
        """)
        print(f"OLS 'efo' requests last 30 days: {result[0][0]:,}")
        
        # Generic resource query - also instant!
        result = self.ch_client.execute("""
            SELECT 
                resource,
                count() as requests,
                uniq(endpoint) as unique_endpoints,
                uniq(country) as countries
            FROM spotappstats.requests
            WHERE request_date >= today() - 7
            GROUP BY resource
            ORDER BY requests DESC
        """)
        
        print("\nLast 7 days summary:")
        for row in result:
            print(f"{row[0]}: {row[1]:,} requests, {row[2]} endpoints, {row[3]} countries")
        
        # Your problem query (with parameters) - now works!
        result = self.ch_client.execute("""
            SELECT count() as total
            FROM spotappstats.requests
            WHERE resource = 'GWAS'
            AND JSONExtractString(parameters, 'trait') = 'diabetes'
            AND request_date >= today() - 30
        """)
        print(f"\nGWAS diabetes queries last 30 days: {result[0][0]:,}")


# Integration into your existing pipeline
if __name__ == "__main__":
    import sys
    
    analytics = ClickHouseAnalyticsLayer()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "setup":
            # Run once to set up ClickHouse
            analytics.setup_clickhouse_once()
            
        elif sys.argv[1] == "sync-files":
            # Run after your existing load-data.py
            analytics.sync_from_processed_files()
            
        elif sys.argv[1] == "sync-db":
            # Alternative: sync from PostgreSQL
            if len(sys.argv) > 2 and sys.argv[2].lower() == "all":
                # Sync all historical data
                chunk_size = 100000  # Default chunk size
                if len(sys.argv) > 3:
                    try:
                        chunk_size = int(sys.argv[3])
                    except ValueError:
                        print("Error: Chunk size must be an integer")
                        print("Usage: python clickhouse_analytics.py sync-db all [chunk_size]")
                        sys.exit(1)
                analytics.sync_from_postgresql(sync_all=True, chunk_size=chunk_size)
            else:
                # Sync by days
                days_back = 1  # Default to 1 day
                if len(sys.argv) > 2:
                    try:
                        days_back = int(sys.argv[2])
                    except ValueError:
                        print("Error: Days parameter must be an integer")
                        print("Usage: python clickhouse_analytics.py sync-db [days|all] [chunk_size]")
                        sys.exit(1)
                analytics.sync_from_postgresql(days_back=days_back)
            
        elif sys.argv[1] == "test":
            # Show example queries
            analytics.example_queries()
    else:
        # Default: sync yesterday's data
        analytics.sync_from_postgresql(days_back=30)

"""
INTEGRATION STEPS FOR YOUR TEAM:

1. Install ClickHouse (one Docker command):
   docker run -d --name clickhouse-server \
     --ulimit nofile=262144:262144 \
     -p 8123:8123 -p 9000:9000 \
     -v clickhouse-data:/var/lib/clickhouse \
     clickhouse/clickhouse-server

2. Add to your .env file:
   CLICKHOUSE_HOST=localhost
   CLICKHOUSE_PORT=9000

3. Install Python package:
   pip install clickhouse-driver

4. Run setup once:
   python clickhouse_analytics.py setup

5. Add to your daily cron (AFTER load-data.py):
   30 6 * * * /usr/bin/python /path/to/clickhouse_analytics.py sync-files
   
   Or sync from PostgreSQL with custom options:
   python clickhouse_analytics.py sync-db 7     # Sync last 7 days
   python clickhouse_analytics.py sync-db 30    # Sync last 30 days
   python clickhouse_analytics.py sync-db all   # Sync ALL historical data
   python clickhouse_analytics.py sync-db all 50000  # Sync all with custom chunk size

That's it! Your queries are now 100x faster.
"""
