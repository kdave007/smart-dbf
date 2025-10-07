from src.db.sqlite_pool import SQLiteConnectionPool
from src.utils.logging_controller import LoggingController
from typing import List, Optional, Dict, Any

class Cut:
    """
    Cut model for database operations on mapeo_cortes table
    """
    
    def __init__(self, db_pool: SQLiteConnectionPool):
        """
        Initialize Cut model with database pool
        
        Args:
            db_pool: SQLiteConnectionPool instance
        """
        self.db_pool = db_pool
        self.logger = LoggingController.get_instance()
        self.table_name = "mapeo_cortes"
    
    def create_table(self) -> bool:
        """
        Create the mapeo_cortes table if it doesn't exist
        
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
                CREATE TABLE IF NOT EXISTS mapeo_cortes (
                    corte_id TEXT PRIMARY KEY NOT NULL,
                    fecha_inicio TEXT NOT NULL,
                    insertado TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """
            
            self.db_pool.execute_update(query)
            self.logger.info("mapeo_cortes table created or already exists")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating mapeo_cortes table: {str(e)}")
            return False
    
    def insert(self, corte_id: str, fecha_inicio: str) -> bool:
        """
        Insert a new cut record
        
        Args:
            corte_id: Unique cut identifier (e.g., 'C202401')
            fecha_inicio: Cut start date 'YYYY-MM-DD'
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
                INSERT INTO mapeo_cortes (corte_id, fecha_inicio)
                VALUES (?, ?)
            """
            
            rows_affected = self.db_pool.execute_update(query, (corte_id, fecha_inicio))
            
            if rows_affected > 0:
                self.logger.info(f"Inserted cut '{corte_id}' starting on {fecha_inicio}")
                return True
            else:
                self.logger.error(f"Failed to insert cut '{corte_id}'")
                return False
                
        except Exception as e:
            self.logger.error(f"Error inserting cut '{corte_id}': {str(e)}")
            return False
    
    def find_by_date(self, reference_date: str) -> Optional[Dict[str, Any]]:
        """
        Find the most recent cut for a given date
        
        Args:
            reference_date: Date string in format 'YYYY-MM-DD'
            
        Returns:
            Cut dictionary or None if not found
        """
        try:
            query = """
                SELECT corte_id, fecha_inicio, insertado
                FROM mapeo_cortes 
                WHERE fecha_inicio <= ?
                ORDER BY fecha_inicio DESC
                LIMIT 1
            """
            
            results = self.db_pool.execute_query(query, (reference_date,))
            
            if results:
                cut_data = dict(results[0])
                self.logger.info(f"Found cut '{cut_data['corte_id']}' for date {reference_date}")
                return cut_data
            else:
                self.logger.warning(f"No cut found for date {reference_date}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error finding cut for date {reference_date}: {str(e)}")
            return None
    
    def find_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Find cuts within a date range
        
        Args:
            start_date: Start date string in format 'YYYY-MM-DD'
            end_date: End date string in format 'YYYY-MM-DD'
            
        Returns:
            List of cut dictionaries
        """
        try:
            query = """
                SELECT corte_id, fecha_inicio, insertado
                FROM mapeo_cortes 
                WHERE fecha_inicio >= ? AND fecha_inicio <= ?
                ORDER BY fecha_inicio ASC
            """
            
            results = self.db_pool.execute_query(query, (start_date, end_date))
            cuts = [dict(row) for row in results]
            
            self.logger.info(f"Found {len(cuts)} cuts for date range {start_date} to {end_date}")
            return cuts
            
        except Exception as e:
            self.logger.error(f"Error finding cuts for date range {start_date}-{end_date}: {str(e)}")
            return []
    
    def find_by_id(self, corte_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a cut by its ID
        
        Args:
            corte_id: Cut identifier
            
        Returns:
            Cut dictionary or None if not found
        """
        try:
            query = """
                SELECT corte_id, fecha_inicio, insertado
                FROM mapeo_cortes 
                WHERE corte_id = ?
            """
            
            results = self.db_pool.execute_query(query, (corte_id,))
            
            if results:
                cut_data = dict(results[0])
                self.logger.info(f"Found cut '{corte_id}'")
                return cut_data
            else:
                self.logger.warning(f"Cut '{corte_id}' not found")
                return None
                
        except Exception as e:
            self.logger.error(f"Error finding cut '{corte_id}': {str(e)}")
            return None
    
    def get_all(self) -> List[Dict[str, Any]]:
        """
        Get all cuts
        
        Returns:
            List of all cut dictionaries
        """
        try:
            query = """
                SELECT corte_id, fecha_inicio, insertado
                FROM mapeo_cortes 
                ORDER BY fecha_inicio DESC
            """
            
            results = self.db_pool.execute_query(query)
            cuts = [dict(row) for row in results]
            
            self.logger.info(f"Retrieved {len(cuts)} cuts")
            return cuts
            
        except Exception as e:
            self.logger.error(f"Error retrieving all cuts: {str(e)}")
            return []
    
    def delete_by_id(self, corte_id: str) -> bool:
        """
        Delete a cut by its ID
        
        Args:
            corte_id: Cut identifier to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = "DELETE FROM mapeo_cortes WHERE corte_id = ?"
            rows_affected = self.db_pool.execute_update(query, (corte_id,))
            
            if rows_affected > 0:
                self.logger.info(f"Deleted cut '{corte_id}'")
                return True
            else:
                self.logger.warning(f"Cut '{corte_id}' not found for deletion")
                return False
                
        except Exception as e:
            self.logger.error(f"Error deleting cut '{corte_id}': {str(e)}")
            return False
