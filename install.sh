#!/bin/bash
# Open Data Platform - Clean Architecture Install
# Run from the open_data directory AFTER running SQL in Supabase

echo "🏗️  Installing clean architecture..."

# Copy updated unified_data.py
cp ingestion/unified_data.py ingestion/unified_data.py.bak 2>/dev/null
cp updated/ingestion/unified_data.py ingestion/

# Copy all pages
cp updated/pages/01_explorer.py web/pages/
cp updated/pages/02_timeseries.py web/pages/
cp updated/pages/03_map.py web/pages/
cp updated/pages/04_economic_crisis.py web/pages/
cp updated/pages/06_fed_data.py web/pages/
cp updated/pages/07_disasters.py web/pages/
cp updated/pages/08_worldbank.py web/pages/

# Remove broken modules
echo "🗑️  Removing broken modules..."
rm -f ingestion/emdat_disasters.py 2>/dev/null
rm -f ingestion/reinhart_rogoff.py 2>/dev/null
rm -f ingestion/fred_data.py 2>/dev/null

# Remove temp folder
rm -rf updated/

# Commit and push
echo "📤 Pushing to GitHub..."
git add .
git commit -m "Clean architecture: rename tables, fix all pages, remove broken modules"
git push origin main

echo ""
echo "✅ DONE!"
echo ""
echo "Final architecture:"
echo "  📊 time_series_unified_data  - All time series (WB, FRED, IMF, etc.)"
echo "  🌪️  event_level_unified_data - Disaster events"
echo ""
echo "Wait 1-2 minutes, then refresh:"
echo "  https://open-data-platform-j8dhonjowzuknwqxvsgjec.streamlit.app"
