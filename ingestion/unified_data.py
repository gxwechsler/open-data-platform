"""Unified Data Access Layer - Single interface to time_series_unified_data."""
import pandas as pd
from database.connection import get_db_manager


class UnifiedData:
    """Access layer for time_series_unified_data table."""
    
    def __init__(self):
        self.db = get_db_manager()
        self.table = "time_series_unified_data"
    
    def get_sources(self) -> list:
        """Get all available data sources."""
        result = self.db.execute_query(
            f"SELECT DISTINCT source FROM {self.table} ORDER BY source"
        )
        return [r['source'] for r in result] if result else []
    
    def get_categories(self, source: str = None) -> list:
        """Get categories, optionally filtered by source."""
        query = f"SELECT DISTINCT category FROM {self.table} WHERE category IS NOT NULL"
        params = {}
        if source:
            query += " AND source = :source"
            params['source'] = source
        query += " ORDER BY category"
        result = self.db.execute_query(query, params if params else None)
        return [r['category'] for r in result] if result else []
    
    def get_indicators(self, source: str = None, category: str = None) -> list:
        """Get indicators with metadata, optionally filtered."""
        query = f"""
            SELECT DISTINCT indicator_code, indicator_name, source, category, units
            FROM {self.table} WHERE 1=1
        """
        params = {}
        if source:
            query += " AND source = :source"
            params['source'] = source
        if category:
            query += " AND category = :category"
            params['category'] = category
        query += " ORDER BY indicator_name"
        return self.db.execute_query(query, params if params else None) or []
    
    def get_countries(self, source: str = None) -> list:
        """Get countries with names, optionally filtered by source."""
        query = f"SELECT DISTINCT country_iso3, country_name FROM {self.table} WHERE 1=1"
        params = {}
        if source:
            query += " AND source = :source"
            params['source'] = source
        query += " ORDER BY country_name"
        return self.db.execute_query(query, params if params else None) or []
    
    def get_year_range(self, source: str = None) -> tuple:
        """Get min/max years available."""
        query = f"SELECT MIN(year) as min_year, MAX(year) as max_year FROM {self.table}"
        params = {}
        if source:
            query += " WHERE source = :source"
            params['source'] = source
        result = self.db.execute_query(query, params if params else None)
        if result and result[0]['min_year']:
            return int(result[0]['min_year']), int(result[0]['max_year'])
        return 1970, 2024
    
    def get_data(self, indicator_code: str = None, countries: list = None,
                 year_start: int = None, year_end: int = None,
                 source: str = None) -> pd.DataFrame:
        """Get time series data with filters."""
        query = f"SELECT * FROM {self.table} WHERE 1=1"
        params = {}
        
        if indicator_code:
            query += " AND indicator_code = :indicator"
            params['indicator'] = indicator_code
        if source:
            query += " AND source = :source"
            params['source'] = source
        if countries:
            placeholders = ', '.join([f":c{i}" for i in range(len(countries))])
            query += f" AND country_iso3 IN ({placeholders})"
            for i, c in enumerate(countries):
                params[f'c{i}'] = c
        if year_start:
            query += " AND year >= :year_start"
            params['year_start'] = year_start
        if year_end:
            query += " AND year <= :year_end"
            params['year_end'] = year_end
        
        query += " ORDER BY country_iso3, year"
        result = self.db.execute_query(query, params if params else None)
        return pd.DataFrame(result) if result else pd.DataFrame()
    
    def search_indicators(self, search_term: str) -> list:
        """Search indicators by name."""
        query = f"""
            SELECT DISTINCT indicator_code, indicator_name, source, category, units
            FROM {self.table}
            WHERE LOWER(indicator_name) LIKE :term
            ORDER BY indicator_name
        """
        result = self.db.execute_query(query, {'term': f'%{search_term.lower()}%'})
        return result or []
    
    def get_summary_stats(self) -> dict:
        """Get summary statistics."""
        query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT source) as sources,
                COUNT(DISTINCT indicator_code) as indicators,
                COUNT(DISTINCT country_iso3) as countries,
                MIN(year) as min_year,
                MAX(year) as max_year
            FROM {self.table}
        """
        result = self.db.execute_query(query)
        return result[0] if result else {}
