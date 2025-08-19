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

# Database setup - with 15 minute query timeout
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "options": "-c statement_timeout=900s"  # 15 minutes timeout for queries
    }
)
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
    ontologyId: str = Query(None, description="[OLS ONLY] The presence of an ontology name triggers a comprehensive, optimized search."),
    country: str = Query(None, description="Country to filter by"),
    endpoint: str = Query(None, description="General endpoint path to filter by (used only in generic search)."),
    parameters: str = Query(None, description="JSON encoded key-value pairs of parameters (used only in generic search).")
):
    """
    Search endpoint statistics. If resource is 'OLS' and an 'ontology' is provided,
    a comprehensive, optimized search is performed. Otherwise, a generic search is used.
    """
    try:
        with get_db() as db:
            params = {
                "resource_name": resource_name,
                "start_date": start_date,
                "end_date": end_date,
                "ontologyId": ontologyId,
                "country": country,
            }

            # Helper to build reusable filter clauses for dates and country
            filter_clauses = ""
            if start_date:
                filter_clauses += " AND r.request_date >= :start_date"
            if end_date:
                filter_clauses += " AND r.request_date <= :end_date"
            if country and country.upper() != 'ALL':
                filter_clauses += " AND r.country_id = (SELECT id FROM countries WHERE name = :country LIMIT 1)"

            # =================================================================
            # OLS COMPREHENSIVE SEARCH
            # Triggered only when resource_name is OLS and an ontology is specified.
            # =================================================================
            if resource_name.upper() == 'OLS' and ontologyId:
                logger.info(f"Executing OLS Comprehensive Search for ontology: '{ontologyId}'")
                query = f"""
                WITH path_matches AS (
                    -- Find requests where the ontology is in the URL path using tsquery
                    SELECT r.id 
                    FROM requests r JOIN endpoints e ON r.endpoint_id = e.id
                    WHERE r.resource_id = (SELECT id FROM resources WHERE name = 'OLS')
                    AND e.path_tsv @@ to_tsquery('ols_search.ols_custom', :ontologyId)
                    {filter_clauses}
                ),
                parameter_matches AS (
                    -- Find requests where the ontology is in the parameters table
                    SELECT r.id 
                    FROM requests r JOIN parameters p ON r.id = p.request_id AND r.request_date = p.request_date
                    WHERE r.resource_id = (SELECT id FROM resources WHERE name = 'OLS')
                    AND p.param_name = 'ontologyId' AND p.param_value = :ontologyId
                    {filter_clauses}
                )
                -- Combine both sets for a final, unique count
                SELECT 'OLS' as resource_name, COUNT(DISTINCT final_id) as matching_requests
                FROM (
                    SELECT id as final_id FROM path_matches
                    UNION
                    SELECT id as final_id FROM parameter_matches
                ) AS final;
                """
            
            # =================================================================
            # GENERIC SEARCH (FALLBACK FOR ALL OTHER CASES)
            # =================================================================
            else:
                logger.info("Executing Generic Search")
                params["endpoint"] = f"{endpoint}%" if endpoint else None

                base_query = ""
                # Handle 'ALL' resources case
                if resource_name.upper() == 'ALL':
                    base_query = "SELECT 'ALL' as resource_name, COUNT(r.id) as matching_requests FROM requests r WHERE 1=1"
                    if "resource_name" in params: del params["resource_name"]
                # Handle a specific resource
                else:
                    base_query = "SELECT res.name as resource_name, COUNT(r.id) as matching_requests FROM requests r JOIN resources res ON r.resource_id = res.id WHERE res.name = :resource_name"

                generic_filters = filter_clauses
                
                # Add endpoint filter if provided
                if endpoint:
                    # Join endpoints table only when needed
                    base_query = base_query.replace(" FROM requests r ", " FROM requests r JOIN endpoints e ON r.endpoint_id = e.id ")
                    generic_filters += " AND e.path LIKE :endpoint"

                # Add parameters filter if provided
                if parameters:
                    try:
                        param_dict = json.loads(parameters)
                        for idx, (key, value) in enumerate(param_dict.items()):
                            param_key = f"param_key_{idx}"
                            param_value = f"param_value_{idx}"
                            generic_filters += f" AND EXISTS (SELECT 1 FROM parameters p WHERE p.request_id = r.id AND p.request_date = r.request_date AND p.param_name = :{param_key} AND p.param_value = :{param_value})"
                            params[param_key] = key
                            params[param_value] = value
                    except json.JSONDecodeError:
                        raise HTTPException(status_code=400, detail="Invalid parameters format.")

                query = base_query + generic_filters
                
                # Add GROUP BY for non-'ALL' queries
                if resource_name.upper() != 'ALL':
                    query += " GROUP BY res.name"


            # Execute the constructed query
            result = db.execute(text(query), params).fetchone()
            
            if not result or result[1] == 0:
                raise HTTPException(status_code=404, detail="No matching data found for the given criteria")
                
            return {
                "resource": result[0],
                "matching_requests": result[1]
            }
            
    except Exception as e:
        logger.error(f"Error in search_stats: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while processing your request")