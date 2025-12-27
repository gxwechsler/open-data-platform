"""Admin Panel - Data Quality & Category Management (Hidden from main menu)."""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Admin | Open Data Platform", page_icon="⚙️", layout="wide")

# Simple password protection
ADMIN_PASSWORD = "opendata2025"

if 'admin_authenticated' not in st.session_state:
    st.session_state.admin_authenticated = False

if not st.session_state.admin_authenticated:
    st.title("🔐 Admin Access")
    password = st.text_input("Enter admin password:", type="password")
    if st.button("Login"):
        if password == ADMIN_PASSWORD:
            st.session_state.admin_authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    st.stop()

# Admin Panel
st.title("⚙️ Admin Panel")
st.markdown("Data Quality & Category Management")

db = get_db_manager()

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🏷️ Category Issues", "🔄 Duplicates", "✏️ Edit Categories"])

with tab1:
    st.markdown("### Database Overview")
    
    # Summary stats
    query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT source) as sources,
            COUNT(DISTINCT country_iso3) as countries,
            COUNT(DISTINCT indicator_code) as indicators,
            COUNT(DISTINCT category) as categories,
            MIN(year) as min_year,
            MAX(year) as max_year
        FROM unified_indicators
    """
    result = db.execute_query(query)
    if result:
        stats = result[0]
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", f"{stats['total_records']:,}")
        with col2:
            st.metric("Sources", stats['sources'])
        with col3:
            st.metric("Countries", stats['countries'])
        with col4:
            st.metric("Indicators", stats['indicators'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Categories", stats['categories'])
        with col2:
            st.metric("Year Range", f"{stats['min_year']}-{stats['max_year']}")
    
    # Records by source
    st.markdown("### Records by Source")
    query = """
        SELECT source, COUNT(*) as records, 
               COUNT(DISTINCT indicator_code) as indicators,
               COUNT(DISTINCT country_iso3) as countries
        FROM unified_indicators 
        GROUP BY source 
        ORDER BY records DESC
    """
    result = db.execute_query(query)
    if result:
        st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
    
    # Records by category
    st.markdown("### Records by Category")
    query = """
        SELECT category, COUNT(*) as records, 
               COUNT(DISTINCT indicator_code) as indicators
        FROM unified_indicators 
        GROUP BY category 
        ORDER BY records DESC
    """
    result = db.execute_query(query)
    if result:
        st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)

with tab2:
    st.markdown("### Category Issues")
    
    # Indicators with NULL category
    st.markdown("#### Indicators with Missing Category")
    query = """
        SELECT DISTINCT source, indicator_code, indicator_name, COUNT(*) as records
        FROM unified_indicators 
        WHERE category IS NULL
        GROUP BY source, indicator_code, indicator_name
        ORDER BY source, indicator_name
    """
    result = db.execute_query(query)
    if result:
        st.warning(f"Found {len(result)} indicators without category")
        st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
    else:
        st.success("All indicators have categories assigned!")
    
    # Similar category names (potential duplicates)
    st.markdown("#### All Current Categories")
    query = """
        SELECT category, COUNT(DISTINCT indicator_code) as indicators, COUNT(*) as records
        FROM unified_indicators 
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY category
    """
    result = db.execute_query(query)
    if result:
        st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
        st.info("Review for similar names like 'Economy' vs 'Economic', 'Health' vs 'Healthcare', etc.")

with tab3:
    st.markdown("### Potential Duplicate Indicators")
    st.markdown("Same indicator code in different categories or sources")
    
    # Same indicator code, different categories
    query = """
        SELECT indicator_code, indicator_name, 
               STRING_AGG(DISTINCT source, ', ') as sources,
               STRING_AGG(DISTINCT category, ', ') as categories,
               COUNT(DISTINCT category) as num_categories
        FROM unified_indicators
        GROUP BY indicator_code, indicator_name
        HAVING COUNT(DISTINCT category) > 1
        ORDER BY num_categories DESC
    """
    result = db.execute_query(query)
    if result:
        st.warning(f"Found {len(result)} indicators in multiple categories")
        st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
    else:
        st.success("No indicators found in multiple categories!")
    
    # Similar indicator names (potential duplicates across sources)
    st.markdown("#### Indicators by Name Pattern")
    search_term = st.text_input("Search indicator names:", placeholder="e.g., GDP, unemployment")
    if search_term:
        query = f"""
            SELECT source, indicator_code, indicator_name, category, COUNT(*) as records
            FROM unified_indicators
            WHERE LOWER(indicator_name) LIKE LOWER('%{search_term}%')
            GROUP BY source, indicator_code, indicator_name, category
            ORDER BY indicator_name, source
        """
        result = db.execute_query(query)
        if result:
            st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
        else:
            st.info("No matching indicators found")

with tab4:
    st.markdown("### Edit Categories")
    st.markdown("Update category assignments for indicators")
    
    # Select indicator to edit
    query = """
        SELECT DISTINCT source, indicator_code, indicator_name, category
        FROM unified_indicators
        ORDER BY source, indicator_name
    """
    result = db.execute_query(query)
    
    if result:
        df = pd.DataFrame(result)
        
        # Filter by source
        sources = ["All"] + sorted(df['source'].unique().tolist())
        filter_source = st.selectbox("Filter by Source:", sources)
        
        if filter_source != "All":
            df = df[df['source'] == filter_source]
        
        # Select indicator
        indicator_options = {f"{row['source']}|{row['indicator_code']}": f"{row['indicator_name']} ({row['source']}) - Current: {row['category']}" 
                           for _, row in df.iterrows()}
        
        selected = st.selectbox("Select Indicator to Edit:", 
                               options=list(indicator_options.keys()),
                               format_func=lambda x: indicator_options.get(x, x))
        
        if selected:
            source, indicator_code = selected.split("|")
            current_row = df[(df['source'] == source) & (df['indicator_code'] == indicator_code)].iloc[0]
            
            st.markdown(f"**Current Category:** {current_row['category']}")
            
            # Category options
            existing_categories = ["Economy", "Labor", "Prices", "Population", "Poverty", 
                                 "Health", "Education", "Infrastructure", "Environment", 
                                 "Finance", "Energy", "Conflict", "Migration"]
            
            new_category = st.selectbox("New Category:", existing_categories,
                                       index=existing_categories.index(current_row['category']) if current_row['category'] in existing_categories else 0)
            
            # Or create new category
            custom_category = st.text_input("Or enter new category name:")
            
            final_category = custom_category if custom_category else new_category
            
            if st.button("Update Category", type="primary"):
                update_query = f"""
                    UPDATE unified_indicators 
                    SET category = '{final_category}'
                    WHERE source = '{source}' AND indicator_code = '{indicator_code}'
                """
                try:
                    db.execute_query(update_query)
                    st.success(f"Updated {indicator_code} to category: {final_category}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error updating: {e}")
    
    st.markdown("---")
    st.markdown("### Bulk Category Update")
    st.markdown("Update multiple indicators at once using SQL")
    
    with st.expander("Advanced: Run Custom SQL"):
        custom_sql = st.text_area("SQL Query:", 
                                  placeholder="UPDATE unified_indicators SET category = 'NewCategory' WHERE indicator_code = 'XX.XXX.XXX'",
                                  height=100)
        if st.button("Execute SQL"):
            if custom_sql.strip().upper().startswith("UPDATE") or custom_sql.strip().upper().startswith("SELECT"):
                try:
                    result = db.execute_query(custom_sql)
                    if result:
                        st.dataframe(pd.DataFrame(result), use_container_width=True)
                    st.success("Query executed successfully")
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.error("Only SELECT and UPDATE queries are allowed")

# Logout
st.sidebar.markdown("---")
if st.sidebar.button("Logout"):
    st.session_state.admin_authenticated = False
    st.rerun()
