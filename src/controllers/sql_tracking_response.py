
from ..models.sql_records import SQLRecords
from ..utils.logging_controller import logging
from ..utils.sql_identifiers_manager import SQLIdentifiersManager
from ..utils.config_manager import ConfigManager

class SQLTrackingResponse:
    def __init__(self, table_name) -> None:
        self.table_name = table_name
        self.sql_records = SQLRecords()
        self.sql_identifiers_manager = SQLIdentifiersManager()
        self.config_manager = ConfigManager.get_instance()
    
    def process_api_response(self, api_response, records_sent, field_id, version, operation_type):
        """Main method to process API response and insert or update tracking records"""
        if not api_response or not records_sent:
            logging.warning("No API response or records to process")
            return False
        
        # Check if API call was successful
        if api_response.get('status_code') != 200:
            logging.error(f"API call failed with status: {api_response.get('status_code')}")
            logging.error(f"Error: {api_response.get('msg', 'Unknown error')}")
            return False
        
        # Match response with sent records and insert tracking data
        success = self._match_batch_and_response(api_response, records_sent, field_id, version, operation_type)
        return success
    
    def _match_batch_and_response(self, api_resp, records_sent, field_id, version, operation_type):
        """Match the response id_cola with the batch and insert tracking records"""
        try:
            # Get id_cola from API response
            id_cola = api_resp.get('id_cola')
            status_id = api_resp.get('status_id', 1)  # Default to 1 if not provided
            
            # Get schema type and conditionally get identifier field
            schema_type = self.sql_identifiers_manager.get_schema_type(self.table_name)
            id_field = field_id  # Default to the field_id parameter
            reference_field_name = None
            
            if schema_type and schema_type != 'natural_key':
                reference_field_name = self.config_manager.get_identifier_field(self.table_name)
                
                    # logging.info(f"Schema type: {schema_type}, Using identifier field: {id_field}") 
            
            print(f"-- SQL lite :: Processing {operation_type} response - ID Cola: {id_cola}, Status: {status_id}")
            
            # Insert records with tracking information
            success = False

            if operation_type == "CREATE":
                success = self.sql_records.insert_sent_data(
                    table_name=self.table_name,
                    records=records_sent,
                    field_id=field_id,
                    version=version,
                    delete_flag=0,  # Not deleted yet even with delete operation
                    waiting_id=id_cola,
                    reference_field_name=reference_field_name,
                    status=status_id  # Status from API response
                )
            
            elif operation_type == "UPDATE":
                success = self.sql_records.update_sent_data(
                    table_name=self.table_name,
                    records=records_sent,
                    field_id=field_id,
                    version=version,
                    waiting_id=id_cola,
                    status=status_id  # Status from API response
                )

            elif operation_type == "DELETE":
                success = self.sql_records.delete_sent_data(
                    table_name=self.table_name,
                    meta_records=records_sent,
                    field_id=field_id,
                    version=version,
                    waiting_id=id_cola,
                    status=status_id  # Status from API response
                )
            
            if success:
                logging.info(f"-- Successfully tracked {len(records_sent)} {operation_type} records with (batch) id_cola: {id_cola}")
            else:
                logging.error(f"-- Failed to track {operation_type} records")
                
            return success
            
        except Exception as e:
            logging.error(f"Error matching batch and response: {str(e)}")
            return False

    def post_insert_records_status(self, api_response, new_records, field_id, version):
        """Insert tracking records for new/created records"""
        return self.process_api_response(api_response, new_records, field_id, version, "CREATE")

    def post_update_records_status(self, api_response, changed_records, field_id, version):
        """Insert tracking records for updated records"""
        return self.process_api_response(api_response, changed_records, field_id, version, "UPDATE")

    def post_delete_records_status(self, api_response, deleted_records, field_id, version):
        """Insert tracking records for deleted records"""
        return self.process_api_response(api_response, deleted_records, field_id, version, "DELETE")

    def get_update_records_status(self):
        """TODO: Get records status from API to update local tracking"""
        # This would make a GET request to check status of records in queue
        pass
