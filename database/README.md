# Database Layer for Open Data Platform

PostgreSQL database with SQLAlchemy ORM for storing and querying economic, financial, and climate data.

## 📦 Quick Setup

### 1. Install Dependencies

```bash
pip install sqlalchemy psycopg2-binary pandas
```

### 2. Create PostgreSQL Database

```bash
# Using psql
createdb open_data

# Or using SQL
psql -c "CREATE DATABASE open_data;"
```

### 3. Configure Connection

Set environment variables:

```bash
# Option A: Full connection string
export DATABASE_URL=postgresql://postgres:password@localhost:5432/open_data

# Option B: Individual variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=open_data
export DB_USER=postgres
export DB_PASSWORD=yourpassword
```

### 4. Initialize Database

```bash
# Create tables and load reference data
python -m database.init_db --init
```

### 5. Load Data

```bash
# Load sample data (no API keys needed)
python -m database.loaders.fred_loader --sample
python -m database.loaders.disaster_loader --sample
python -m database.loaders.crisis_loader --sample

# Load live FRED data (requires API key)
export FRED_API_KEY=your_api_key
python -m database.loaders.fred_loader --all
```

## 📊 Tables

| Table | Description |
|-------|-------------|
| `countries` | Reference data (45+ countries) |
| `indicators` | Indicator metadata catalog |
| `economic_data` | World Bank, IMF time series |
| `fed_series` | FRED monetary data |
| `fed_series_meta` | FRED series metadata |
| `disasters` | EM-DAT natural disasters |
| `financial_crises` | Banking, currency, sovereign crises |

## 🔧 Usage

### Python

```python
from database import get_db, Country, Disaster, FinancialCrisis

db = get_db()

with db.session() as session:
    # Query countries
    usa = session.query(Country).filter_by(iso3="USA").first()
    print(usa.name)  # "United States"
    
    # Query disasters
    earthquakes = session.query(Disaster).filter_by(
        disaster_type=DisasterType.EARTHQUAKE
    ).order_by(Disaster.deaths.desc()).limit(10).all()
    
    # Query crises
    banking_crises = session.query(FinancialCrisis).filter_by(
        crisis_type=CrisisType.BANKING
    ).all()
```

### SQL (Direct)

```sql
-- Most affected countries by disasters
SELECT 
    c.name,
    COUNT(*) as disaster_count,
    SUM(d.deaths) as total_deaths
FROM disasters d
JOIN countries c ON d.country_iso3 = c.iso3
GROUP BY c.name
ORDER BY disaster_count DESC
LIMIT 10;

-- Banking crises with highest output loss
SELECT 
    c.name,
    fc.start_year,
    fc.output_loss_pct,
    fc.fiscal_cost_pct
FROM financial_crises fc
JOIN countries c ON fc.country_iso3 = c.iso3
WHERE fc.crisis_type = 'BANKING'
ORDER BY fc.output_loss_pct DESC
LIMIT 10;
```

## 📁 Structure

```
database/
├── __init__.py          # Package exports
├── connection.py        # Database connection manager
├── models.py            # SQLAlchemy ORM models
├── init_db.py           # Initialization script
├── loaders/
│   ├── __init__.py
│   ├── fred_loader.py   # FRED data loader
│   ├── disaster_loader.py  # EM-DAT loader
│   └── crisis_loader.py    # Crisis data loader
└── README.md
```

## 🔑 API Keys

| Source | URL | Required For |
|--------|-----|--------------|
| FRED | https://fred.stlouisfed.org/docs/api/api_key.html | Live monetary data |
| EM-DAT | https://public.emdat.be | Full disaster database |

Sample data works without any API keys.

## 🐳 Docker (Optional)

```yaml
# docker-compose.yml
version: '3.8'
services:
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: open_data
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

```bash
docker-compose up -d
export DATABASE_URL=postgresql://postgres:password@localhost:5432/open_data
python -m database.init_db --init
```

## 📈 Statistics

After loading sample data:

| Table | Rows |
|-------|------|
| countries | 45 |
| indicators | 20 |
| fed_series | ~3,000 |
| disasters | 45 |
| financial_crises | 50 |
