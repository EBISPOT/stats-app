from fastapi import FastAPI, HTTPException, Query, Depends
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Optional, Dict
from datetime import datetime, date
import jwt
import logging
import json
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import contextmanager
import os
from dotenv import load_dotenv
from pathlib import Path

# Logger configuration
logger = logging.getLogger("api_logger")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Load .env from root directory
root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / '.env')

# Database config
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

# API Configuration
app = FastAPI(title="App Stats API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Models
class ResourceStats(BaseModel):
    resource_name: str
    total_requests: int
    unique_endpoints: int
    top_endpoints: List[Dict[str, str]]

class ParameterStats(BaseModel):
    param_name: str
    frequency: int
    top_values: List[Dict[str, str]]

# Database dependency
@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# API Routes
@app.get("/api/resources", response_model=List[str])
def get_resources():
    """Get list of all available resources"""
    with get_db() as db:
        result = db.execute(text("SELECT name FROM resources ORDER BY name"))
        return [row[0] for row in result]

@app.get("/api/resources/{resource_name}/stats")
def get_resource_stats(
    resource_name: str,
    start_date: date = Query(None),
    end_date: date = Query(None)
):
    """Get statistics for a specific resource"""
    with get_db() as db:
        resource_name = resource_name.upper()
        # Base query for resource stats
        query = """
        WITH resource_data AS (
            SELECT 
                r.id,
                e.path,
                COUNT(*) as request_count
            FROM requests r
            JOIN resources res ON r.resource_id = res.id
            JOIN endpoints e ON r.endpoint_id = e.id
            WHERE res.name = :resource_name
            {date_filter}
            GROUP BY r.id, e.path
        )
        SELECT
            COUNT(*) as total_requests,
            COUNT(DISTINCT path) as unique_endpoints,
            ARRAY_AGG(DISTINCT path) FILTER (WHERE path IN (
                SELECT path
                FROM resource_data
                GROUP BY path
                ORDER BY COUNT(*) DESC
                LIMIT 5
            )) as top_endpoints
        FROM resource_data
        """
        
        params = {"resource_name": resource_name}
        date_filter = []
        
        if start_date:
            date_filter.append("r.request_date >= :start_date")
            params["start_date"] = start_date
        if end_date:
            date_filter.append("r.request_date <= :end_date")
            params["end_date"] = end_date
            
        if date_filter:
            query = query.format(date_filter="AND " + " AND ".join(date_filter))
        else:
            query = query.format(date_filter="")
            
        result = db.execute(text(query), params).fetchone()
        
        return {
            "resource_name": resource_name,
            "total_requests": result[0],
            "unique_endpoints": result[1],
            "top_endpoints": [{"path": path} for path in (result[2] or [])]
        }

@app.get("/api/resources/{resource_name}/parameters")
def get_parameter_stats(
    resource_name: str,
    start_date: date = Query(None),
    end_date: date = Query(None)
):
    """Get parameter statistics for a resource"""
    with get_db() as db:
        query = """
        SELECT 
            p.param_name,
            COUNT(*) as frequency,
            ARRAY_AGG(p.param_value) FILTER (
                WHERE param_value IN (
                    SELECT param_value
                    FROM parameters p2
                    WHERE p2.param_name = p.param_name
                    GROUP BY param_value
                    ORDER BY COUNT(*) DESC
                    LIMIT 5
                )
            ) as top_values
        FROM parameters p
        JOIN requests r ON p.request_id = r.id
        JOIN resources res ON r.resource_id = res.id
        WHERE res.name = :resource_name
        {date_filter}
        GROUP BY p.param_name
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        params = {"resource_name": resource_name}
        date_filter = []
        
        if start_date:
            date_filter.append("r.request_date >= :start_date")
            params["start_date"] = start_date
        if end_date:
            date_filter.append("r.request_date <= :end_date")
            params["end_date"] = end_date
            
        if date_filter:
            query = query.format(date_filter="AND " + " AND ".join(date_filter))
        else:
            query = query.format(date_filter="")
            
        results = db.execute(text(query), params).fetchall()
        
        return [
            {
                "param_name": row[0],
                "frequency": row[1],
                "top_values": [{"value": val} for val in (row[2] or [])]
            }
            for row in results
        ]

@app.get("/api/resources/{resource_name}/timeline")
def get_request_timeline(
    resource_name: str,
    start_date: date = Query(None),
    end_date: date = Query(None),
    interval: str = Query("day", regex="^(hour|day|week|month)$")
):
    """Get request timeline for a resource"""
    with get_db() as db:
        interval_sql = {
            "hour": "DATE_TRUNC('hour', request_timestamp)",
            "day": "DATE_TRUNC('day', request_timestamp)",
            "week": "DATE_TRUNC('week', request_timestamp)",
            "month": "DATE_TRUNC('month', request_timestamp)"
        }[interval]
        
        query = f"""
        SELECT 
            {interval_sql} as time_bucket,
            COUNT(*) as request_count
        FROM requests r
        JOIN resources res ON r.resource_id = res.id
        WHERE res.name = :resource_name
        {" AND r.request_date >= :start_date" if start_date else ""}
        {" AND r.request_date <= :end_date" if end_date else ""}
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        
        params = {"resource_name": resource_name}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
            
        results = db.execute(text(query), params).fetchall()
        
        return [
            {
                "timestamp": row[0].isoformat(),
                "count": row[1]
            }
            for row in results
        ]

@app.get("/api/countries", response_model=List[str])
def get_countries():
    """Get list of all available countries"""
    with get_db() as db:
        query = """
        SELECT DISTINCT name 
        FROM countries 
        WHERE name IS NOT NULL 
        ORDER BY name
        """
        result = db.execute(text(query))
        return [row[0] for row in result]


@app.get("/api/stats/search")
def search_stats(
    resource_name: str = Query(..., description="Name of the resource to query"),
    start_date: date = Query(None, description="Start date for the search period"),
    end_date: date = Query(None, description="End date for the search period"),
    endpoint: str = Query(None, description="Endpoint path to filter by"),
    country: str = Query(None, description="Country to filter by"),
    parameters: str = Query(None, description="JSON encoded key-value pairs of parameters to filter by")
):
    """
    Search endpoint statistics with filters for resource, date range, endpoint, and parameters.
    Supports 'ALL' as resource_name to query across all resources and 'ALL' for country to query all countries.
    """
    try:
        with get_db() as db:
            if resource_name.upper() == 'ALL':
                # Query for ALL resources
                query = """
                SELECT 'ALL' as resource_name, COUNT(*) as matching_requests
                FROM requests r
                JOIN resources res ON r.resource_id = res.id
                WHERE 1=1
                """
            else:
                # Query for specific resource
                query = """
                SELECT res.name as resource_name, COUNT(*) as matching_requests
                FROM requests r
                JOIN resources res ON r.resource_id = res.id
                WHERE res.name = :resource_name
                """
            
            # Initialize parameters dictionary
            params = {}
            if resource_name.upper() != 'ALL':
                params["resource_name"] = resource_name.upper()
            
            # Add date range filters
            if start_date:
                query += " AND r.request_date >= :start_date"
                params["start_date"] = start_date
            if end_date:
                query += " AND r.request_date <= :end_date"
                params["end_date"] = end_date
                
            # Add endpoint filter using EXISTS for better performance on large endpoint table
            if endpoint:
                if resource_name.upper() == 'ALL':
                    query += """ AND EXISTS (
                        SELECT 1 FROM endpoints e 
                        WHERE e.id = r.endpoint_id 
                        AND e.path LIKE :endpoint
                    )"""
                else:
                    query += """ AND EXISTS (
                        SELECT 1 FROM endpoints e 
                        WHERE e.id = r.endpoint_id 
                        AND e.resource_id = res.id
                        AND e.path LIKE :endpoint
                    )"""
                params["endpoint"] = f"%{endpoint}%"

            # Add country filter only if not 'ALL'
            if country and country.upper() != 'ALL':
                query += """ 
                AND EXISTS (
                    SELECT 1 FROM countries c 
                    WHERE c.id = r.country_id 
                    AND c.name = :country
                )
                """
                params["country"] = country
                
            # Add parameters filter if provided
            if parameters:
                try:
                    param_dict = json.loads(parameters)
                    for idx, (key, value) in enumerate(param_dict.items()):
                        param_key = f"param_key_{idx}"
                        param_value = f"param_value_{idx}"
                        query += f"""
                            AND EXISTS (
                                SELECT 1 
                                FROM parameters p 
                                WHERE p.request_id = r.id 
                                AND p.request_date = r.request_date
                                AND p.param_name = :{param_key}
                                AND p.param_value = :{param_value}
                            )
                        """
                        params[param_key] = key
                        params[param_value] = value
                except json.JSONDecodeError:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid parameters format. Expected JSON string."
                    )

            # Add GROUP BY
            if resource_name.upper() == 'ALL':
                query += " GROUP BY 1"
            else:
                query += " GROUP BY res.name"
            
            result = db.execute(text(query), params).fetchone()
            
            if not result or result[1] == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No matching data found for the given criteria"
                )
                
            return {
                "resource": result[0],
                "matching_requests": result[1]
            }
            
    except Exception as e:
        logger.error(f"Error in search_stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing your request"
        )