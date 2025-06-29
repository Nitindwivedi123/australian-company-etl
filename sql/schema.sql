-- ./sql/schema.sql

-- staging_commoncrawl
CREATE TABLE IF NOT EXISTS staging_commoncrawl (
    id SERIAL PRIMARY KEY,
    website_url TEXT,
    company_name TEXT,
    industry TEXT,
    created_at TIMESTAMP DEFAULT now()
);

-- staging_abr
CREATE TABLE IF NOT EXISTS staging_abr (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(20) UNIQUE,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address TEXT,
    postcode VARCHAR(10),
    state VARCHAR(10),
    start_date DATE,
    created_at TIMESTAMP DEFAULT now()
);

-- unified_companies
CREATE TABLE IF NOT EXISTS unified_companies (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(20) UNIQUE,  
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address TEXT,
    postcode VARCHAR(10),
    state VARCHAR(10),
    start_date DATE,
    website_url TEXT,
    company_name TEXT,
    industry TEXT,
    merged_confidence INT,
    created_at TIMESTAMP DEFAULT now()
);

-- processed_files
CREATE TABLE IF NOT EXISTS processed_files (
    id SERIAL PRIMARY KEY,
    file_path TEXT UNIQUE NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- indexes (use IF NOT EXISTS for Postgres > 9.5)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_staging_commoncrawl_name
  ON staging_commoncrawl
  USING gin (to_tsvector('english', company_name));

CREATE INDEX IF NOT EXISTS idx_staging_abr_name
  ON staging_abr
  USING gin (to_tsvector('english', entity_name));

CREATE INDEX IF NOT EXISTS idx_unified_entity_trgm
  ON unified_companies
  USING gin (entity_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_unified_cc_trgm
  ON unified_companies
  USING gin (company_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_unified_postcode
  ON unified_companies (postcode);
