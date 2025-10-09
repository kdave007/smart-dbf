from ..models.sql_records import SQLRecords
from ..utils.sql_identifiers_manager import SQLIdentifiersManager
from ..utils.data_tables_schemas_manager import DataTablesSchemasManager
import sys


class SQLReferences:
    def __init__(self, table_name) -> None:
        self.set_table(table_name)
        self.sql_records = SQLRecords()
        self.sql_id_manager = SQLIdentifiersManager()
        self.data_table_schema_manager = DataTablesSchemasManager()

    def set_table(self, table_name) -> None:
        self.table_name = table_name

    def _get_by_batches(self, dbf_records,batch_size=500):
        """by batches depending on the records lentgth, fetch from the sqlite the matching records by the specific id given field name"""
        #here i need to divide the records 
        version = self.sql_id_manager.get_batch_version()
        schema_type = self.sql_id_manager.get_schema_type(self.table_name)
        field_name = self.data_table_schema_manager.get_id_field_name(schema_type)
        
        all_results = {}
        
        # all_results = self.sql_records.select_all_records(self.table_name, 100)

        # # Process records in batches
        for i in range(0, len(dbf_records), batch_size):
            records_batch = dbf_records[i:i + batch_size]
          
            batch_results = self.sql_records.batch_select_by_id(records_batch, field_name, self.table_name, version, len(records_batch))
            all_results.update(batch_results)
        
        return all_results
    