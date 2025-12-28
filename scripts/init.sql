-- Open Data Platform - Database Initialization Script
-- This script creates the base schema with partitioned tables for optimal performance

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Countries (45 countries organized by region)
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    iso3_code VARCHAR(3) UNIQUE NOT NULL,
    iso2_code VARCHAR(2) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    subregion VARCHAR(50),
    income_level VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Data Sources (World Bank, IMF, UNHCR, etc.)
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    base_url VARCHAR(255),
    description TEXT,
    last_updated TIMESTAMP
);

-- Indicator Categories (hierarchical)
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    parent_id INTEGER REFERENCES categories(id),
    description TEXT
);

-- Indicators
CREATE TABLE IF NOT EXISTS indicators (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    category_id INTEGER REFERENCES categories(id),
    code VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    unit VARCHAR(100),
    frequency VARCHAR(20) DEFAULT 'annual',
    UNIQUE(source_id, code)
);

-- ============================================================================
-- FACT TABLE (PARTITIONED BY YEAR)
-- ============================================================================

CREATE TABLE IF NOT EXISTS observations (
    id BIGSERIAL,
    country_id INTEGER NOT NULL,
    indicator_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    value NUMERIC,
    is_estimated BOOLEAN DEFAULT FALSE,
    source_note TEXT,
    fetched_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, year),
    FOREIGN KEY (country_id) REFERENCES countries(id),
    FOREIGN KEY (indicator_id) REFERENCES indicators(id)
) PARTITION BY RANGE (year);

-- Create partitions by decade (1950s through 2030s for future data)
CREATE TABLE IF NOT EXISTS observations_1950s PARTITION OF observations
    FOR VALUES FROM (1950) TO (1960);

CREATE TABLE IF NOT EXISTS observations_1960s PARTITION OF observations
    FOR VALUES FROM (1960) TO (1970);

CREATE TABLE IF NOT EXISTS observations_1970s PARTITION OF observations
    FOR VALUES FROM (1970) TO (1980);

CREATE TABLE IF NOT EXISTS observations_1980s PARTITION OF observations
    FOR VALUES FROM (1980) TO (1990);

CREATE TABLE IF NOT EXISTS observations_1990s PARTITION OF observations
    FOR VALUES FROM (1990) TO (2000);

CREATE TABLE IF NOT EXISTS observations_2000s PARTITION OF observations
    FOR VALUES FROM (2000) TO (2010);

CREATE TABLE IF NOT EXISTS observations_2010s PARTITION OF observations
    FOR VALUES FROM (2010) TO (2020);

CREATE TABLE IF NOT EXISTS observations_2020s PARTITION OF observations
    FOR VALUES FROM (2020) TO (2030);

CREATE TABLE IF NOT EXISTS observations_2030s PARTITION OF observations
    FOR VALUES FROM (2030) TO (2040);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_obs_country_year ON observations(country_id, year);
CREATE INDEX IF NOT EXISTS idx_obs_indicator_year ON observations(indicator_id, year);
CREATE INDEX IF NOT EXISTS idx_obs_country_indicator ON observations(country_id, indicator_id);
CREATE INDEX IF NOT EXISTS idx_obs_year ON observations(year);

CREATE INDEX IF NOT EXISTS idx_indicators_source ON indicators(source_id);
CREATE INDEX IF NOT EXISTS idx_indicators_category ON indicators(category_id);
CREATE INDEX IF NOT EXISTS idx_countries_region ON countries(region);

-- ============================================================================
-- METADATA TABLES
-- ============================================================================

-- Data ingestion tracking
CREATE TABLE IF NOT EXISTS ingestion_logs (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',
    records_processed INTEGER DEFAULT 0,
    error_message TEXT
);

-- User-defined indicator groups
CREATE TABLE IF NOT EXISTS indicator_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS indicator_group_members (
    group_id INTEGER REFERENCES indicator_groups(id) ON DELETE CASCADE,
    indicator_id INTEGER REFERENCES indicators(id) ON DELETE CASCADE,
    PRIMARY KEY (group_id, indicator_id)
);

-- ============================================================================
-- SEED DATA: Sources
-- ============================================================================

INSERT INTO sources (code, name, base_url, description) VALUES
    ('WB', 'World Bank', 'https://api.worldbank.org/v2/', 'World Bank Open Data API'),
    ('IMF', 'International Monetary Fund', 'https://dataservices.imf.org/', 'IMF Data Services'),
    ('UNHCR', 'UN Refugee Agency', 'https://api.unhcr.org/', 'UNHCR Population Statistics'),
    ('ITU', 'International Telecommunication Union', 'https://datahub.itu.int/', 'ICT Statistics'),
    ('UNODC', 'UN Office on Drugs and Crime', 'https://dataunodc.un.org/', 'Crime and Drug Statistics'),
    ('WRI', 'World Resources Institute', 'https://www.wri.org/', 'Environmental Data')
ON CONFLICT (code) DO NOTHING;

-- ============================================================================
-- SEED DATA: Categories
-- ============================================================================

INSERT INTO categories (code, name, description) VALUES
    ('ECONOMIC', 'Economic', 'Economic indicators including GDP, growth, and trade'),
    ('FINANCIAL', 'Financial', 'Financial sector and monetary indicators'),
    ('DEMOGRAPHIC', 'Demographic', 'Population and demographic indicators'),
    ('SOCIAL', 'Social', 'Social development indicators'),
    ('HEALTH', 'Health', 'Health and healthcare indicators'),
    ('EDUCATION', 'Education', 'Education and literacy indicators'),
    ('ENVIRONMENT', 'Environment', 'Environmental and climate indicators'),
    ('INFRASTRUCTURE', 'Infrastructure', 'Infrastructure and digitalization indicators'),
    ('GOVERNANCE', 'Governance', 'Governance and institutional indicators'),
    ('SECURITY', 'Security', 'Security and crime indicators')
ON CONFLICT (code) DO NOTHING;

-- ============================================================================
-- SEED DATA: Countries (45 countries)
-- ============================================================================

INSERT INTO countries (iso3_code, iso2_code, name, region, subregion) VALUES
    -- AMERICA (7)
    ('ARG', 'AR', 'Argentina', 'AMERICA', 'South America'),
    ('BRA', 'BR', 'Brazil', 'AMERICA', 'South America'),
    ('CHL', 'CL', 'Chile', 'AMERICA', 'South America'),
    ('COL', 'CO', 'Colombia', 'AMERICA', 'South America'),
    ('MEX', 'MX', 'Mexico', 'AMERICA', 'North America'),
    ('USA', 'US', 'United States', 'AMERICA', 'North America'),
    ('CAN', 'CA', 'Canada', 'AMERICA', 'North America'),
    -- EUROPE (12)
    ('DEU', 'DE', 'Germany', 'EUROPE', 'Western Europe'),
    ('FRA', 'FR', 'France', 'EUROPE', 'Western Europe'),
    ('ITA', 'IT', 'Italy', 'EUROPE', 'Southern Europe'),
    ('SWE', 'SE', 'Sweden', 'EUROPE', 'Northern Europe'),
    ('NLD', 'NL', 'Netherlands', 'EUROPE', 'Western Europe'),
    ('CHE', 'CH', 'Switzerland', 'EUROPE', 'Western Europe'),
    ('DNK', 'DK', 'Denmark', 'EUROPE', 'Northern Europe'),
    ('FIN', 'FI', 'Finland', 'EUROPE', 'Northern Europe'),
    ('NOR', 'NO', 'Norway', 'EUROPE', 'Northern Europe'),
    ('TUR', 'TR', 'Turkey', 'EUROPE', 'Southern Europe'),
    ('ESP', 'ES', 'Spain', 'EUROPE', 'Southern Europe'),
    ('GBR', 'GB', 'United Kingdom', 'EUROPE', 'Northern Europe'),
    -- ASIA (5)
    ('IND', 'IN', 'India', 'ASIA', 'South Asia'),
    ('CHN', 'CN', 'China', 'ASIA', 'East Asia'),
    ('JPN', 'JP', 'Japan', 'ASIA', 'East Asia'),
    ('VNM', 'VN', 'Vietnam', 'ASIA', 'Southeast Asia'),
    ('SGP', 'SG', 'Singapore', 'ASIA', 'Southeast Asia'),
    -- MIDDLE EAST (5)
    ('ISR', 'IL', 'Israel', 'MIDDLE_EAST', 'Western Asia'),
    ('IRN', 'IR', 'Iran', 'MIDDLE_EAST', 'Western Asia'),
    ('ARE', 'AE', 'United Arab Emirates', 'MIDDLE_EAST', 'Arabian Peninsula'),
    ('SAU', 'SA', 'Saudi Arabia', 'MIDDLE_EAST', 'Arabian Peninsula'),
    ('QAT', 'QA', 'Qatar', 'MIDDLE_EAST', 'Arabian Peninsula'),
    -- AFRICA (11)
    ('NER', 'NE', 'Niger', 'AFRICA', 'West Africa'),
    ('ZAF', 'ZA', 'South Africa', 'AFRICA', 'Southern Africa'),
    ('EGY', 'EG', 'Egypt', 'AFRICA', 'North Africa'),
    ('COD', 'CD', 'Congo (DRC)', 'AFRICA', 'Central Africa'),
    ('MAR', 'MA', 'Morocco', 'AFRICA', 'North Africa'),
    ('DZA', 'DZ', 'Algeria', 'AFRICA', 'North Africa'),
    ('ETH', 'ET', 'Ethiopia', 'AFRICA', 'East Africa'),
    ('LBY', 'LY', 'Libya', 'AFRICA', 'North Africa'),
    ('TZA', 'TZ', 'Tanzania', 'AFRICA', 'East Africa'),
    ('TUN', 'TN', 'Tunisia', 'AFRICA', 'North Africa'),
    ('GHA', 'GH', 'Ghana', 'AFRICA', 'West Africa'),
    -- SOUTH PACIFIC (2)
    ('AUS', 'AU', 'Australia', 'SOUTH_PACIFIC', 'Oceania'),
    ('NZL', 'NZ', 'New Zealand', 'SOUTH_PACIFIC', 'Oceania')
ON CONFLICT (iso3_code) DO NOTHING;

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Latest observations by country and indicator
CREATE OR REPLACE VIEW latest_observations AS
SELECT DISTINCT ON (o.country_id, o.indicator_id)
    c.iso3_code,
    c.name AS country_name,
    c.region,
    i.code AS indicator_code,
    i.name AS indicator_name,
    cat.name AS category,
    o.year,
    o.value,
    o.is_estimated
FROM observations o
JOIN countries c ON o.country_id = c.id
JOIN indicators i ON o.indicator_id = i.id
LEFT JOIN categories cat ON i.category_id = cat.id
ORDER BY o.country_id, o.indicator_id, o.year DESC;

-- View: Country summary with observation counts
CREATE OR REPLACE VIEW country_summary AS
SELECT
    c.iso3_code,
    c.name,
    c.region,
    COUNT(DISTINCT o.indicator_id) AS indicator_count,
    COUNT(o.id) AS observation_count,
    MIN(o.year) AS earliest_year,
    MAX(o.year) AS latest_year
FROM countries c
LEFT JOIN observations o ON c.id = o.country_id
GROUP BY c.id, c.iso3_code, c.name, c.region;

COMMENT ON TABLE countries IS 'Reference table for 45 selected countries';
COMMENT ON TABLE indicators IS 'Catalog of available indicators from all data sources';
COMMENT ON TABLE observations IS 'Time series data partitioned by decade for performance';
