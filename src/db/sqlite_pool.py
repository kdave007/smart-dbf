import sqlite3
import threading
from queue import Queue, Empty
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager
from src.utils.logging_controller import LoggingController

class SQLiteConnectionPool:
    """
    SQLite connection pool manager for thread-safe database operations.
    Manages a pool of SQLite connections to avoid connection overhead.
    """
    
    def __init__(self, database_path: str, pool_size: int = 5, timeout: int = 30):
        """
        Initialize SQLite connection pool.
        
        Args:
            database_path: Path to SQLite database file
            pool_size: Maximum number of connections in pool
            timeout: Connection timeout in seconds
        """
        self.database_path = Path(database_path)
        self.pool_size = pool_size
        self.timeout = timeout
        self.pool = Queue(maxsize=pool_size)
        self.active_connections = 0
        self.lock = threading.Lock()
        self.logger = LoggingController.get_instance()
        
        # Create database directory if it doesn't exist
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize the pool
        self._initialize_pool()
        
    def _initialize_pool(self):
        """Initialize the connection pool with connections."""
        try:
            for _ in range(self.pool_size):
                conn = self._create_connection()
                if conn:
                    self.pool.put(conn)
                    self.active_connections += 1
            
            print(f"SQLite pool initialized with {self.active_connections} connections")
            
        except Exception as e:
            self.logger.error(f"Error initializing SQLite pool: {str(e)}")
            raise
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new SQLite connection with optimal settings."""
        try:
            conn = sqlite3.connect(
                str(self.database_path),
                timeout=self.timeout,
                check_same_thread=False
            )
            
            # Set optimal SQLite settings
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            
            # Enable row factory for dict-like access
            conn.row_factory = sqlite3.Row
            
            return conn
            
        except Exception as e:
            self.logger.error(f"Error creating SQLite connection: {str(e)}")
            return None
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool.
        
        Returns:
            SQLite connection from pool
            
        Raises:
            Exception: If no connection available within timeout
        """
        try:
            # Try to get connection from pool
            conn = self.pool.get(timeout=self.timeout)
            
            # Test connection is still valid
            try:
                conn.execute("SELECT 1")
                return conn
            except sqlite3.Error:
                # Connection is stale, create new one
                self.logger.warning("Stale connection detected, creating new one")
                conn.close()
                new_conn = self._create_connection()
                if new_conn:
                    return new_conn
                else:
                    raise Exception("Failed to create new connection")
                    
        except Empty:
            # Pool is empty, try to create new connection if under limit
            with self.lock:
                if self.active_connections < self.pool_size:
                    conn = self._create_connection()
                    if conn:
                        self.active_connections += 1
                        return conn
            
            raise Exception(f"No connections available within {self.timeout} seconds")
    
    def return_connection(self, conn: sqlite3.Connection):
        """
        Return a connection to the pool.
        
        Args:
            conn: SQLite connection to return to pool
        """
        try:
            if conn and not self.pool.full():
                # Test connection before returning to pool
                try:
                    conn.execute("SELECT 1")
                    self.pool.put_nowait(conn)
                except sqlite3.Error:
                    # Connection is bad, close it
                    conn.close()
                    with self.lock:
                        self.active_connections -= 1
            else:
                # Pool is full or connection is None, close it
                if conn:
                    conn.close()
                    with self.lock:
                        self.active_connections -= 1
                        
        except Exception as e:
            self.logger.error(f"Error returning connection to pool: {str(e)}")
            if conn:
                conn.close()
                with self.lock:
                    self.active_connections -= 1
    
    @contextmanager
    def get_connection_context(self):
        """
        Context manager for getting and automatically returning connections.
        
        Usage:
            with pool.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM table")
        """
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        finally:
            if conn:
                self.return_connection(conn)
    
    def execute_query(self, query: str, params: tuple = None) -> list:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            
        Returns:
            List of query results
        """
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            
        Returns:
            Number of affected rows
        """
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
    
    def execute_many(self, query: str, params_list: list) -> int:
        """
        Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            Number of affected rows
        """
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
    
    def close_all(self):
        """Close all connections in the pool."""
        self.logger.info("Closing all SQLite pool connections")
        
        # Close connections in pool
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except Empty:
                break
            except Exception as e:
                self.logger.error(f"Error closing pooled connection: {str(e)}")
        
        with self.lock:
            self.active_connections = 0
        
        self.logger.info("All SQLite pool connections closed")
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get current pool status information.
        
        Returns:
            Dictionary with pool status information
        """
        return {
            'database_path': str(self.database_path),
            'pool_size': self.pool_size,
            'active_connections': self.active_connections,
            'available_connections': self.pool.qsize(),
            'timeout': self.timeout
        }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close all connections."""
        self.close_all()
