-- Open Data Platform - Supabase Database Initialization
-- Run this in Supabase SQL Editor to create all tables

-- ============================================
-- FEDERAL RESERVE DATA TABLES
-- ============================================

-- Fed Series Metadata
CREATE TABLE IF NOT EXISTS fed_series_meta (
    series_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    units VARCHAR(100),
    frequency VARCHAR(20),
    seasonal_adjustment VARCHAR(50),
    last_updated TIMESTAMP,
    observation_start DATE,
    observation_end DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Fed Series Observations
CREATE TABLE IF NOT EXISTS fed_series (
    id BIGSERIAL PRIMARY KEY,
    series_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    value NUMERIC(20, 6),
    country_iso3 VARCHAR(3) DEFAULT 'USA',
    series_name VARCHAR(200),
    units VARCHAR(100),
    frequency VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(series_id, date)
);

CREATE INDEX IF NOT EXISTS ix_fed_series_date ON fed_series(series_id, date);

-- ============================================
-- DISASTER DATA TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS disasters (
    id SERIAL PRIMARY KEY,
    emdat_id VARCHAR(20) UNIQUE,
    country_iso3 VARCHAR(3) NOT NULL,
    disaster_type VARCHAR(50) NOT NULL,
    disaster_group VARCHAR(50),
    disaster_subtype VARCHAR(100),
    event_name VARCHAR(200),
    start_date DATE,
    end_date DATE,
    year INTEGER NOT NULL,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT,
    magnitude FLOAT,
    magnitude_scale VARCHAR(20),
    deaths INTEGER,
    deaths_missing INTEGER,
    injured INTEGER,
    affected BIGINT,
    homeless INTEGER,
    total_affected BIGINT,
    damage_usd NUMERIC(20, 2),
    damage_adjusted NUMERIC(20, 2),
    insured_damage_usd NUMERIC(20, 2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_disasters_type_year ON disasters(disaster_type, year);
CREATE INDEX IF NOT EXISTS ix_disasters_country_year ON disasters(country_iso3, year);

-- ============================================
-- FINANCIAL CRISES TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS financial_crises (
    id SERIAL PRIMARY KEY,
    country_iso3 VARCHAR(3) NOT NULL,
    crisis_type VARCHAR(50) NOT NULL,
    start_year INTEGER NOT NULL,
    end_year INTEGER,
    peak_year INTEGER,
    source VARCHAR(50) NOT NULL,
    source_notes TEXT,
    output_loss_pct FLOAT,
    fiscal_cost_pct FLOAT,
    monetary_expansion_pct FLOAT,
    peak_npl_pct FLOAT,
    exchange_rate_depreciation FLOAT,
    peak_inflation FLOAT,
    peak_month VARCHAR(20),
    haircut_pct FLOAT,
    external_default BOOLEAN,
    domestic_default BOOLEAN,
    duration_years FLOAT,
    resolution_type VARCHAR(100),
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso3, crisis_type, start_year)
);

CREATE INDEX IF NOT EXISTS ix_crises_type_year ON financial_crises(crisis_type, start_year);
CREATE INDEX IF NOT EXISTS ix_crises_country_year ON financial_crises(country_iso3, start_year);

-- ============================================
-- GRANT PERMISSIONS (for Supabase)
-- ============================================

-- Enable Row Level Security (optional, for public read access)
-- ALTER TABLE fed_series ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fed_series_meta ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE disasters ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE financial_crises ENABLE ROW LEVEL SECURITY;

-- Create policies for public read access (if using RLS)
-- CREATE POLICY "Public read access" ON fed_series FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON fed_series_meta FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON disasters FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON financial_crises FOR SELECT USING (true);

-- ============================================
-- SAMPLE DATA (Optional - for testing)
-- ============================================

-- Insert sample FRED metadata
INSERT INTO fed_series_meta (series_id, name, category, units, frequency) VALUES
('GDP', 'Gross Domestic Product', 'National Accounts', 'Billions of Dollars', 'Quarterly'),
('UNRATE', 'Unemployment Rate', 'Labor Market', 'Percent', 'Monthly'),
('CPIAUCSL', 'Consumer Price Index', 'Prices', 'Index 1982-1984=100', 'Monthly'),
('FEDFUNDS', 'Federal Funds Effective Rate', 'Interest Rates', 'Percent', 'Monthly'),
('DGS10', '10-Year Treasury Rate', 'Interest Rates', 'Percent', 'Monthly')
ON CONFLICT (series_id) DO NOTHING;

-- Insert sample disasters
INSERT INTO disasters (emdat_id, country_iso3, disaster_type, disaster_group, event_name, year, start_date, magnitude, deaths, total_affected, damage_usd, latitude, longitude) VALUES
('2010-0016-HTI', 'HTI', 'EARTHQUAKE', 'GEOPHYSICAL', 'Haiti Earthquake', 2010, '2010-01-12', 7.0, 222570, 3700000, 8000000000, 18.45, -72.45),
('2011-0077-JPN', 'JPN', 'EARTHQUAKE', 'GEOPHYSICAL', 'Tohoku Earthquake and Tsunami', 2011, '2011-03-11', 9.1, 19846, 368820, 210000000000, 38.3, 142.4),
('2005-0324-USA', 'USA', 'TROPICAL_CYCLONE', 'METEOROLOGICAL', 'Hurricane Katrina', 2005, '2005-08-29', 5, 1833, 500000, 125000000000, 29.95, -90.07),
('2023-0175-TUR', 'TUR', 'EARTHQUAKE', 'GEOPHYSICAL', 'Turkey-Syria Earthquake', 2023, '2023-02-06', 7.8, 50783, 26000000, 34200000000, 37.2, 37.0)
ON CONFLICT (emdat_id) DO NOTHING;

-- Insert sample crises
INSERT INTO financial_crises (country_iso3, crisis_type, start_year, end_year, source, output_loss_pct, fiscal_cost_pct, description) VALUES
('USA', 'BANKING', 2007, 2009, 'LAEVEN_VALENCIA', 31.0, 4.5, 'Subprime mortgage crisis leading to global financial crisis'),
('USA', 'BANKING', 1929, 1933, 'REINHART_ROGOFF', 45.0, NULL, 'Great Depression banking crisis'),
('GRC', 'SOVEREIGN', 2010, 2018, 'LAEVEN_VALENCIA', 43.0, 27.3, 'Greek sovereign debt crisis'),
('THA', 'BANKING', 1997, 2000, 'LAEVEN_VALENCIA', 109.0, 43.8, 'Thai banking crisis - origin of Asian financial crisis')
ON CONFLICT (country_iso3, crisis_type, start_year) DO NOTHING;

-- Verify tables were created
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
