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

Drop table requests;

-- Create partitions for requests (example for 2024)
CREATE TABLE requests_2024_01 PARTITION OF requests
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE requests_2024_02 PARTITION OF requests
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE requests_2024_11 PARTITION OF requests
    FOR VALUES FROM ('2024-11-01') TO ('2024-11-30');
-- ... continue for other months

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

-- Create a function to automatically create new partitions
CREATE OR REPLACE FUNCTION create_partition_if_not_exists()
    RETURNS trigger AS $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    partition_date := DATE_TRUNC('month', NEW.request_date);
    partition_name := 'requests_' || TO_CHAR(partition_date, 'YYYY_MM');
    start_date := partition_date;
    end_date := partition_date + INTERVAL '1 month';

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
                'CREATE TABLE %I PARTITION OF requests FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                start_date,
                end_date
                );

        -- Create indexes on the new partition
        EXECUTE format(
                'CREATE INDEX %I ON %I (request_date)',
                'idx_' || partition_name || '_date',
                partition_name
                );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for automatic partition creation
CREATE TRIGGER create_partition_trigger
    BEFORE INSERT ON requests
    FOR EACH ROW
EXECUTE FUNCTION create_partition_if_not_exists();

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