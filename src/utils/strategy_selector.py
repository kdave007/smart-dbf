import json
from pathlib import Path

class StrategySelector:

    def get(self, table_name):
        """
            receives the table name and seeks the schema params in rules.json and mappings.json
        """
        try:
            rules_path = Path(__file__).parent / "rules.json"
            with open(rules_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                table_config = data.get(table_name, {})
                strategy = table_config.get('strategy', 'simple')  # Default to 'simple'
                return strategy
        except Exception as e:
            print(f"[StrategySelector] Error loading rules.json: {e}")
            return 'error'  # Default fallback