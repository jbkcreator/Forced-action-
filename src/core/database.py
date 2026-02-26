"""
Database connection and session management for the application.
Provides utilities for database initialization and session handling.
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import Pool

from config.settings import get_settings
from src.core.models import Base


# ============================================================================
# DATABASE ENGINE & SESSION FACTORY
# ============================================================================

class Database:
    """
    Singleton database manager that handles engine creation and session management.
    """

    _instance = None
    _engine: Engine = None
    _session_factory: sessionmaker = None

    def __new__(cls):
        """Ensure only one instance of Database exists."""
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the database connection if not already done."""
        if self._engine is None:
            self._initialize_engine()

    def _initialize_engine(self) -> None:
        """Create the SQLAlchemy engine with connection pooling."""
        settings = get_settings()

        self._engine = create_engine(
            settings.database_url,
            echo=settings.db_echo,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,  # Enable connection health checks
            pool_recycle=3600,  # Recycle connections after 1 hour
        )

        # Configure session factory
        self._session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )

        # Set up event listeners
        self._setup_event_listeners()

    def _setup_event_listeners(self) -> None:
        """Set up SQLAlchemy event listeners for connection management."""

        @event.listens_for(Pool, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            """Set PostgreSQL connection parameters if needed."""
            # This can be used to set PostgreSQL-specific parameters
            # For now, it's a placeholder for future customization
            pass

        @event.listens_for(Pool, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Called when a connection is retrieved from the pool."""
            # Useful for logging or monitoring
            pass

    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy engine."""
        if self._engine is None:
            self._initialize_engine()
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get the session factory."""
        if self._session_factory is None:
            self._initialize_engine()
        return self._session_factory

    def create_all_tables(self) -> None:
        """
        Create all tables in the database.
        This should be used carefully, preferably only during initial setup.
        For production, use Alembic migrations instead.
        """
        Base.metadata.create_all(bind=self._engine)

    def drop_all_tables(self) -> None:
        """
        Drop all tables from the database.
        WARNING: This will delete all data! Use with extreme caution.
        """
        Base.metadata.drop_all(bind=self._engine)

    def get_session(self) -> Session:
        """
        Get a new database session.
        Remember to close the session after use or use the session context manager.
        """
        return self._session_factory()

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope for database operations.

        Usage:
            with db.session_scope() as session:
                session.add(new_property)
                # Commit happens automatically if no exception
                # Rollback happens automatically on exception
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        """Close the database engine and dispose of the connection pool."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None


# ============================================================================
# GLOBAL DATABASE INSTANCE
# ============================================================================

# Singleton instance for application-wide use
db = Database()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_db_session() -> Session:
    """
    Get a new database session.
    Useful for dependency injection in frameworks like FastAPI.

    Usage:
        session = get_db_session()
        try:
            # Use session
            pass
        finally:
            session.close()
    """
    return db.get_session()


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """
    Get a database session with automatic cleanup.

    Usage:
        with get_db_context() as session:
            properties = session.query(Property).all()
    """
    with db.session_scope() as session:
        yield session


def init_database() -> None:
    """
    Initialize the database by creating all tables.
    Use this only for development or initial setup.
    For production, use Alembic migrations.
    """
    db.create_all_tables()


def reset_database() -> None:
    """
    Drop and recreate all tables.
    WARNING: This will delete all data! Use only in development.
    """
    db.drop_all_tables()
    db.create_all_tables()


# ============================================================================
# DATABASE UTILITIES
# ============================================================================

def check_connection() -> bool:
    """
    Check if the database connection is working.
    Returns True if connection is successful, False otherwise.
    """
    try:
        with db.session_scope() as session:
            # Execute a simple query to test connection
            session.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


def get_table_counts() -> dict:
    """
    Get the count of records in each table.
    Useful for debugging and monitoring.
    """
    from src.core.models import (
        Property, Owner, Financial, CodeViolation, LegalAndLien, Deed, LegalProceeding,
        TaxDelinquency, Foreclosure, BuildingPermit, Incident, DistressScore
    )

    counts = {}
    with db.session_scope() as session:
        counts['properties'] = session.query(Property).count()
        counts['owners'] = session.query(Owner).count()
        counts['financials'] = session.query(Financial).count()
        counts['code_violations'] = session.query(CodeViolation).count()
        counts['legal_and_liens'] = session.query(LegalAndLien).count()
        counts['deeds'] = session.query(Deed).count()
        counts['legal_proceedings'] = session.query(LegalProceeding).count()
        counts['tax_delinquencies'] = session.query(TaxDelinquency).count()
        counts['foreclosures'] = session.query(Foreclosure).count()
        counts['building_permits'] = session.query(BuildingPermit).count()
        counts['incidents'] = session.query(Incident).count()
        counts['distress_scores'] = session.query(DistressScore).count()

    return counts


if __name__ == "__main__":

    print(get_table_counts())