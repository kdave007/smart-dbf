from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseIdentifier(ABC):
    def __init__(self, table_name: str, cut_id: str = None):
        self.cut_id = cut_id
        self.table_name = table_name

    @abstractmethod
    def calculate_identifier(self, record: Dict) -> Any:
        """
        Calculate identifier value for a single record
        Different for each strategy
        """
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """
        Returns the strategy type
        """
        pass

