from src.models.cut import Cut
from src.db.sqlite_pool import SQLiteConnectionPool
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

class CutManager:
    """
    Controller for managing cuts - handles business logic and delegates to Cut model
    """
    
    def __init__(self, db_pool: SQLiteConnectionPool):
        """
        Initialize CutManager with SQLite connection pool
        
        Args:
            db_pool: SQLiteConnectionPool instance for database operations
        """
        self.cut_model = Cut(db_pool)
        
        # Ensure table exists
        self.cut_model.create_table()
    
    def get_cut_for_date(self, reference_date: str) -> Optional[str]:
        """
        Gets active cut_id for a given date
        Example: '2024-03-15' -> 'C202401'
        
        Args:
            reference_date: Date string in format 'YYYY-MM-DD'
            
        Returns:
            Cut ID string or None if not found
        """
        cut_data = self.cut_model.find_by_date(reference_date)
        return cut_data['corte_id'] if cut_data else None
    
    def get_cuts_for_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        For a date range, there could be multiple cuts
        Returns list of cuts covering the range
        
        Args:
            start_date: Start date string in format 'YYYY-MM-DD'
            end_date: End date string in format 'YYYY-MM-DD'
            
        Returns:
            List of cut dictionaries with cut information
        """
        return self.cut_model.find_by_date_range(start_date, end_date)
    
    def create_cut(self, corte_id: str, fecha_inicio: str) -> bool:
        """
        Create a new cut record
        
        Args:
            corte_id: Unique cut identifier (e.g., 'C202401')
            fecha_inicio: Cut start date 'YYYY-MM-DD'
            
        Returns:
            True if successful, False otherwise
        """
        return self.cut_model.insert(corte_id, fecha_inicio)
    
   
    
   