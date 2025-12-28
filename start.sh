#!/bin/bash
echo "🚀 Starting Open Data Platform..."

cd ~/open_data
source venv/bin/activate
export DATABASE_URL=postgresql://localhost:5432/open_data

echo "📊 Checking database..."
psql -d open_data -c "SELECT 'Database connected!' as status;" 2>/dev/null || echo "⚠️  Database not running"

echo "🌐 Starting Streamlit..."
streamlit run web/app.py
