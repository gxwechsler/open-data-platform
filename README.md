# 📊 Open Data Platform

A Streamlit web application for exploring economic indicators, financial crises, and natural disasters.

## Features

- **📈 Economic Indicators** - Federal Reserve (FRED) time series data
- **🏦 Financial Crises** - 800+ years of banking, currency, and sovereign debt crises
- **🌪️ Natural Disasters** - Global disaster data from EM-DAT
- **🗺️ Interactive Maps** - Geographic visualizations
- **📊 Analysis Tools** - Twin crisis detection, trend analysis, comparisons

## Quick Start

### Option 1: Run with Sample Data (No Database Required)

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/open-data-platform.git
cd open-data-platform

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run web/app.py
```

The app works out of the box with built-in sample data!

### Option 2: Deploy to Streamlit Cloud with Supabase

See [Deployment Guide](#deployment) below.

## Project Structure

```
open_data/
├── web/
│   ├── app.py              # Main Streamlit app
│   └── pages/              # Streamlit pages
│       ├── 01_explorer.py
│       ├── 02_timeseries.py
│       ├── 03_map.py
│       ├── 04_crises.py
│       ├── 05_crisis_analysis.py
│       ├── 06_fed_data.py
│       └── 07_disasters.py
├── database/
│   ├── connection.py       # Database connection manager
│   ├── models.py           # SQLAlchemy models
│   └── supabase_init.sql   # SQL to initialize Supabase
├── ingestion/
│   ├── fred_data.py        # FRED data module
│   ├── emdat_disasters.py  # Disaster data module
│   └── reinhart_rogoff.py  # Crisis data module
├── .streamlit/
│   ├── config.toml
│   └── secrets.toml.example
├── requirements.txt
└── README.md
```

## Deployment

### Step 1: Create Supabase Project

1. Go to [supabase.com](https://supabase.com) and create a free account
2. Create a new project
3. Go to **Project Settings → Database**
4. Copy the **Connection string (URI)** - choose "Transaction pooler"
5. Replace `[YOUR-PASSWORD]` with your database password

### Step 2: Initialize Database

1. In Supabase, go to **SQL Editor**
2. Copy contents of `database/supabase_init.sql`
3. Run the SQL to create tables and sample data

### Step 3: Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/open-data-platform.git
git push -u origin main
```

### Step 4: Deploy to Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Connect your GitHub account
3. Select your repository
4. Set **Main file path**: `web/app.py`
5. In **Advanced settings → Secrets**, add:
   ```toml
   DATABASE_URL = "your-supabase-connection-string"
   ```
6. Click **Deploy**

## Data Sources

- [Federal Reserve Economic Data (FRED)](https://fred.stlouisfed.org/)
- [EM-DAT International Disaster Database](https://www.emdat.be/)
- Reinhart-Rogoff "This Time Is Different" (2009)
- Laeven-Valencia Systemic Banking Crises Database (IMF)

## License

MIT License
