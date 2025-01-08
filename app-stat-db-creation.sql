-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create lookup tables first
CREATE TABLE resources (
                           id SERIAL PRIMARY KEY,
                           name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE endpoints (
                           id SERIAL PRIMARY KEY,
                           path TEXT NOT NULL,
                           resource_id INTEGER NOT NULL REFERENCES resources(id),
                           UNIQUE(path, resource_id)
);

CREATE TABLE countries (
                           id SERIAL PRIMARY KEY,
                           name VARCHAR(255) NOT NULL UNIQUE
);

-- Create the main requests table with partitioning
CREATE TABLE requests (
                          id BIGSERIAL,
                          request_date DATE NOT NULL,
                          resource_id INTEGER NOT NULL REFERENCES resources(id),
                          endpoint_id INTEGER NOT NULL REFERENCES endpoints(id),
                          request_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                          country_id INTEGER REFERENCES countries(id),
                          PRIMARY KEY (id, request_date),
                          UNIQUE (request_date, request_timestamp, endpoint_id, resource_id)
) PARTITION BY RANGE (request_date);

-- Create parameters table
CREATE TABLE parameters (
                            id BIGSERIAL PRIMARY KEY,
                            request_id BIGINT NOT NULL,
                            request_date DATE NOT NULL,
                            param_name VARCHAR(100) NOT NULL,
                            param_value TEXT,
                            FOREIGN KEY (request_id, request_date) REFERENCES requests(id, request_date) ON DELETE CASCADE
);

-- Create yearly partitions with broader ranges
CREATE TABLE requests_2024 PARTITION OF requests
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE requests_2025 PARTITION OF requests
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE requests_2026 PARTITION OF requests
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE requests_2027 PARTITION OF requests
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');



-- Create indexes for lookup tables
CREATE INDEX idx_resources_name ON resources(name);
CREATE INDEX idx_endpoints_path ON endpoints USING gin (path gin_trgm_ops);
CREATE INDEX idx_endpoints_resource ON endpoints(resource_id);
CREATE INDEX idx_countries_name ON countries(name);

-- Create indexes for the requests table
CREATE INDEX idx_requests_date ON requests(request_date);
CREATE INDEX idx_requests_resource ON requests(resource_id, request_date);
CREATE INDEX idx_requests_endpoint ON requests(endpoint_id, request_date);
CREATE INDEX idx_requests_country ON requests(country_id, request_date);
CREATE INDEX idx_requests_timestamp ON requests(request_timestamp);

-- Create indexes for the parameters table
CREATE INDEX idx_parameters_request ON parameters(request_id, request_date);
CREATE INDEX idx_parameters_lookup ON parameters(request_id, request_date, param_name, param_value);
CREATE INDEX idx_parameters_name_value ON parameters(param_name, param_value);

-- Create a view for common queries
CREATE VIEW daily_request_stats AS
SELECT
    r.request_date,
    res.name as resource_name,
    e.path as endpoint_path,
    c.name as country_name,
    COUNT(*) as request_count
FROM requests r
         JOIN resources res ON r.resource_id = res.id
         JOIN endpoints e ON r.endpoint_id = e.id
         LEFT JOIN countries c ON r.country_id = c.id
GROUP BY r.request_date, res.name, e.path, c.name;