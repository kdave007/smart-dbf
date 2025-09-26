import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime


class FilterManager:
    def __init__(self, rules_file_path: str = None):
        """
        Initialize Filter Manager
        
        Args:
            rules_file_path: Path to rules.json file (optional)
        """
        # Handle exe-compatible default path
        if rules_file_path is None:
            if getattr(sys, 'frozen', False):
                # Running as exe
                base_path = Path(sys.executable).parent
                self.rules_file_path = str(base_path / "src" / "utils" / "rules.json")
            else:
                # Running in development
                self.rules_file_path = "src/utils/rules.json"
        else:
            self.rules_file_path = rules_file_path
            
        self.rules = self._load_rules()
    
    def _load_rules(self) -> Dict[str, Any]:
        """Load filter rules from JSON file"""
        try:
            with open(self.rules_file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: Rules file not found at {self.rules_file_path}")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing rules file: {e}")
            return {}
    
    def build_filters(self, table_name: str, date_range: Optional[Dict[str, str]] = None, 
                     value_filters: Optional[Dict[str, str]] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Build filters based on table configuration and input parameters
        
        Args:
            table_name: Name of the table
            date_range: Optional date range filter with 'from' and 'to' keys
            value_filters: Optional value filters dict with field names as keys
            
        Returns:
            List of filter dictionaries or None if no filters
        """
        filters = []
        
        # Get all filter configurations for this table
        table_filters = self._get_all_filters_for_table(table_name)
        
        for filter_type, filter_config in table_filters.items():
            # Skip disabled filters
            if not filter_config.get('enabled', 1):
                print(f"[DEBUG] Skipping disabled filter: {filter_type}")
                continue
                
            if filter_type == 'date' and date_range:
                date_filter = self._build_date_filter(filter_config, date_range)
                if date_filter:
                    filters.extend(date_filter)
                    
            elif filter_type == 'value' and value_filters:
                value_filter = self._build_value_filter(filter_config, value_filters)
                if value_filter:
                    filters.extend(value_filter)
        
        return filters if filters else None
    
    def _build_date_filter(self, filter_config: Dict[str, Any], date_range: Dict[str, str]) -> Optional[List[Dict[str, Any]]]:
        """Build date filter based on configuration"""
        if 'from' not in date_range or 'to' not in date_range:
            return None
            
        date_field = filter_config["field"]
        date_format = filter_config.get("format", "%d/%m/%Y")
        condition = filter_config.get("condition", "between")
        
        print(f"[DEBUG] Building date filter: field={date_field}, format={date_format}, condition={condition}")
        
        try:
            from_date = datetime.strptime(date_range['from'], '%Y-%m-%d').strftime(date_format)
            to_date = datetime.strptime(date_range['to'], '%Y-%m-%d').strftime(date_format)
            print(f"[DEBUG] Date filter: {from_date} to {to_date}")
            
            if condition == "between":
                return [{"field": date_field, "operator": "range", "from_value": from_date, "to_value": to_date}]
            elif condition == "equal":
                return [{"field": date_field, "operator": "=", "value": from_date}]
                
        except ValueError as e:
            print(f"[DEBUG] Date conversion error: {e}")
            
        return None
    
    def _build_value_filter(self, filter_config: Dict[str, Any], value_filters: Dict[str, str]) -> Optional[List[Dict[str, Any]]]:
        """Build value filter based on configuration"""
        field_name = filter_config["field"]
        condition = filter_config.get("condition", "equal")
        
        if field_name not in value_filters:
            return None
            
        value = value_filters[field_name]
        print(f"[DEBUG] Building value filter: field={field_name}, condition={condition}, value={value}")
        
        if condition == "equal":
            return [{"field": field_name, "operator": "=", "value": value}]
        elif condition == "contains":
            return [{"field": field_name, "operator": "LIKE", "value": f"%{value}%"}]
        elif condition == "starts_with":
            return [{"field": field_name, "operator": "LIKE", "value": f"{value}%"}]
        elif condition == "ends_with":
            return [{"field": field_name, "operator": "LIKE", "value": f"%{value}"}]
            
        return None
    
    def _get_all_filters_for_table(self, table_name: str) -> Dict[str, Dict[str, Any]]:
        """Get all filter configurations for a specific table from rules"""
        print(f"[DEBUG] Looking up all filters for table: {table_name}")
        print(f"[DEBUG] Available tables in rules: {list(self.rules.keys()) if self.rules else 'None'}")
        
        if not self.rules:
            print("[DEBUG] No table filters loaded, using default")
            return {"date": {"field": "F_EMISION", "format": "%d/%m/%Y", "condition": "between", "enabled": 1}}
        
        # Remove .DBF extension if present for lookup
        table_key = table_name.replace('.DBF', '')
        print(f"[DEBUG] Looking for table key: {table_key}")
        
        table_config = self.rules.get(table_key)
        print(f"[DEBUG] Table config found: {table_config}")
        
        if table_config and 'filters' in table_config:
            filters = table_config['filters']
            print(f"[DEBUG] Returning filters: {filters}")
            return filters
        
        # Default fallback
        print("[DEBUG] Using default fallback config")
        return {"date": {"field": "F_EMISION", "format": "%d/%m/%Y", "condition": "between", "enabled": 1}}
    
    def get_filter_config(self, table_name: str, filter_type: str = "date") -> Dict[str, Any]:
        """Get specific filter configuration for a table"""
        all_filters = self._get_all_filters_for_table(table_name)
        return all_filters.get(filter_type, {"field": "F_EMISION", "format": "%d/%m/%Y", "enabled": 1})
    
    def is_filter_enabled(self, table_name: str, filter_type: str) -> bool:
        """Check if a specific filter is enabled for a table"""
        filter_config = self.get_filter_config(table_name, filter_type)
        return filter_config.get('enabled', 1) == 1
    
    def get_available_filters(self, table_name: str) -> List[str]:
        """Get list of available filter types for a table"""
        all_filters = self._get_all_filters_for_table(table_name)
        return list(all_filters.keys())
