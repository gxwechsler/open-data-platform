"""Database connection manager for Open Data Platform."""
import os
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

def get_database_url():
    """Get database URL from environment or Streamlit secrets."""
    url = os.environ.get('DATABASE_URL')
    if url:
        return url
    
    if HAS_STREAMLIT:
        try:
            url = st.secrets.get('DATABASE_URL')
            if url:
                return url
        except Exception:
            pass
    
    return os.environ.get('LOCAL_DATABASE_URL', 'postgresql://localhost:5432/open_data')

def get_engine(database_url=None):
    """Create SQLAlchemy engine with connection pooling."""
    url = database_url or get_database_url()
    if not url:
        return None
    
    try:
        engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False
        )
        return engine
    except Exception as e:
        if HAS_STREAMLIT:
            st.warning(f"Could not connect to database: {e}")
        return None

@contextmanager
def get_session(engine=None):
    """Context manager for database sessions."""
    if engine is None:
        engine = get_engine()
    
    if engine is None:
        yield None
        return
    
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

class DatabaseManager:
    """Manages database connections and queries."""
    
    def __init__(self, database_url=None):
        self.database_url = database_url or get_database_url()
        self._engine = None
    
    @property
    def engine(self):
        if self._engine is None:
            self._engine = get_engine(self.database_url)
        return self._engine
    
    def is_connected(self):
        """Check if database is connected."""
        if self.engine is None:
            return False
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def execute_query(self, query, params=None):
        """Execute a query and return results as list of dicts."""
        if self.engine is None:
            return []
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception as e:
            if HAS_STREAMLIT:
                st.error(f"Query error: {e}")
            return []
    
    def get_table_count(self, table_name):
        """Get count of rows in a table."""
        result = self.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
        return result[0]['count'] if result else 0

_db_manager = None

def get_db_manager():
    """Get or create the database manager singleton."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager
