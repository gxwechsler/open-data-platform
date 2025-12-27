"""Unified data access module for all indicator sources."""
import pandas as pd
from database.connection import get_db_manager


class UnifiedData:
    """Access unified indicators from all sources."""
    
    def __init__(self):
        self.db = get_db_manager()
    
    def get_sources(self):
        """Get list of available data sources."""
        query = "SELECT DISTINCT source FROM unified_indicators ORDER BY source"
        result = self.db.execute_query(query)
        return [r['source'] for r in result] if result else []
    
    def get_countries(self, source=None):
        """Get list of countries, optionally filtered by source."""
        if source:
            query = f"SELECT DISTINCT country_iso3, country_name FROM unified_indicators WHERE source = '{source}' ORDER BY country_name"
        else:
            query = "SELECT DISTINCT country_iso3, country_name FROM unified_indicators ORDER BY country_name"
        result = self.db.execute_query(query)
        return result if result else []
    
    def get_categories(self, source=None):
        """Get list of categories, optionally filtered by source."""
        if source:
            query = f"SELECT DISTINCT category FROM unified_indicators WHERE source = '{source}' AND category IS NOT NULL ORDER BY category"
        else:
            query = "SELECT DISTINCT category FROM unified_indicators WHERE category IS NOT NULL ORDER BY category"
        result = self.db.execute_query(query)
        return [r['category'] for r in result] if result else []
    
    def get_indicators(self, source=None, category=None):
        """Get list of indicators, optionally filtered by source and category."""
        conditions = []
        if source:
            conditions.append(f"source = '{source}'")
        if category:
            conditions.append(f"category = '{category}'")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT DISTINCT indicator_code, indicator_name, source, category, units
            FROM unified_indicators 
            WHERE {where_clause}
            ORDER BY indicator_name
        """
        result = self.db.execute_query(query)
        return result if result else []
    
    def get_data(self, indicator_code=None, countries=None, source=None, year_start=None, year_end=None):
        """Get indicator data with filters."""
        conditions = []
        
        if indicator_code:
            conditions.append(f"indicator_code = '{indicator_code}'")
        if source:
            conditions.append(f"source = '{source}'")
        if countries:
            if isinstance(countries, list):
                countries_str = "','".join(countries)
                conditions.append(f"country_iso3 IN ('{countries_str}')")
            else:
                conditions.append(f"country_iso3 = '{countries}'")
        if year_start:
            conditions.append(f"year >= {year_start}")
        if year_end:
            conditions.append(f"year <= {year_end}")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT source, country_iso3, country_name, indicator_code, indicator_name, 
                   category, year, value, units
            FROM unified_indicators 
            WHERE {where_clause}
            ORDER BY year, country_name
        """
        result = self.db.execute_query(query)
        return pd.DataFrame(result) if result else pd.DataFrame()
    
    def get_multi_indicator_data(self, indicator_codes, countries=None, year_start=None, year_end=None):
        """Get data for multiple indicators."""
        if not indicator_codes:
            return pd.DataFrame()
        
        conditions = []
        indicators_str = "','".join(indicator_codes)
        conditions.append(f"indicator_code IN ('{indicators_str}')")
        
        if countries:
            if isinstance(countries, list):
                countries_str = "','".join(countries)
                conditions.append(f"country_iso3 IN ('{countries_str}')")
            else:
                conditions.append(f"country_iso3 = '{countries}'")
        if year_start:
            conditions.append(f"year >= {year_start}")
        if year_end:
            conditions.append(f"year <= {year_end}")
        
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT source, country_iso3, country_name, indicator_code, indicator_name, 
                   category, year, value, units
            FROM unified_indicators 
            WHERE {where_clause}
            ORDER BY year, country_name, indicator_code
        """
        result = self.db.execute_query(query)
        return pd.DataFrame(result) if result else pd.DataFrame()
    
    def get_year_range(self, source=None):
        """Get min and max years available."""
        if source:
            query = f"SELECT MIN(year) as min_year, MAX(year) as max_year FROM unified_indicators WHERE source = '{source}'"
        else:
            query = "SELECT MIN(year) as min_year, MAX(year) as max_year FROM unified_indicators"
        result = self.db.execute_query(query)
        if result:
            return result[0]['min_year'], result[0]['max_year']
        return 2000, 2023
    
    def search_indicators(self, search_term):
        """Search indicators by name or code."""
        query = f"""
            SELECT DISTINCT indicator_code, indicator_name, source, category
            FROM unified_indicators 
            WHERE LOWER(indicator_name) LIKE LOWER('%{search_term}%')
               OR LOWER(indicator_code) LIKE LOWER('%{search_term}%')
            ORDER BY indicator_name
            LIMIT 50
        """
        result = self.db.execute_query(query)
        return result if result else []
    
    def get_summary_stats(self):
        """Get summary statistics for the database."""
        query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT source) as sources,
                COUNT(DISTINCT country_iso3) as countries,
                COUNT(DISTINCT indicator_code) as indicators,
                MIN(year) as min_year,
                MAX(year) as max_year
            FROM unified_indicators
        """
        result = self.db.execute_query(query)
        return result[0] if result else {}
