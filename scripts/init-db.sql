-- K-Scrape Nexus Database Initialization Script

-- Create database extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create enum types for job and task statuses
DO $$ BEGIN
    CREATE TYPE job_status_enum AS ENUM ('pending', 'processing', 'completed', 'failed', 'cancelled');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE task_status_enum AS ENUM ('pending', 'processing', 'completed', 'failed', 'retrying');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE worker_type_enum AS ENUM ('http', 'headless', 'processing');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create indexes for better query performance
-- These will be created by Alembic migrations, but we include them here for reference

-- Performance monitoring
SELECT 'Database initialization completed' as status;