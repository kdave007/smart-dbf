
from .identifier_resolver import IdentifierResolver

class DataComparator:
    def __init__(self) -> None:
        pass

    def get_blueprint(self, table_name, dbf_records):
        # Initialize (automatically loads sql_identifiers.json)
        resolver = IdentifierResolver()

        # Resolve strategy for a table
        strategy = resolver.resolve_identifier_strategy(table_name)
        print(f'strategy {strategy.get_strategy_name()} for {table_name}')

        for dbf_record in dbf_records:
            unique_id = strategy.calculate_identifier(dbf_record)
            print(f'record {dbf_record.get('__meta').get('recno')} - calc id : {unique_id}')
        
        return strategy