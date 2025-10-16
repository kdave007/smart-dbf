from src.controllers.operation import Operation
# from src.controllers.operation_raw_og import Operation
from src.controllers.sql_tracking_response import SQLTrackingResponse
import logging

class BatchProcessor:
    def __init__(self, table, config_manager, sql_enabled=False, batch_size=100):
        self.config_manager = config_manager 
        client_id = self.config_manager.config.get('plaza') + "_" + config_manager.config.get('sucursal')
        debug_mode = self.config_manager.get_debug_mode()

        self.operation =  Operation(config_manager.config, table, client_id, simulate_response=debug_mode)
        self.tracking = SQLTrackingResponse(table)
        self.sql_enabled = sql_enabled
        self.batch_size = batch_size
    
    def process_all_operations(self, operations_obj, schema_type, field_id, version):
        """Procesa todas las operaciones usando batches"""
        # Process NEW records in batches
        if operations_obj['new']:
            self._process_in_batches(
                records=operations_obj['new'],
                operation_type="new",
                schema_type=schema_type,
                field_id=field_id,
                version=version,
                send_method=self.operation.send_new_records,
                track_method=self.tracking.post_insert_records_status if self.tracking else None
            )
        
        # Process CHANGED records in batches  
        if operations_obj['changed']:
            self._process_in_batches(
                records=operations_obj['changed'],
                operation_type="updated",
                schema_type=schema_type,
                field_id=field_id,
                version=version,
                send_method=self.operation.send_updates,
                track_method=self.tracking.post_update_records_status if self.tracking else None
            )
        
        # Process DELETED records in batches
        if operations_obj['deleted']:
            self._process_in_batches(
                records=operations_obj['deleted'],
                operation_type="deleted",
                schema_type=schema_type,
                field_id=field_id,
                version=version,
                send_method=self.operation.send_deletes,
                track_method=self.tracking.post_delete_records_status if self.tracking else None
            )
        
        # Unchanged records
        if operations_obj['unchanged']:
            print(f" Unchanged records ({len(operations_obj['unchanged'])}) - no action needed")
    
    def _process_in_batches(self, records, operation_type, schema_type, field_id, version, send_method, track_method):
        """Procesa registros en batches del tamaño configurado"""
        total_records = len(records)
        successful_batches = 0
        
        logging.info(f" Processing {total_records} {operation_type} records in batches of {self.batch_size}...")
        
        for i in range(0, total_records, self.batch_size):
            batch = records[i:i + self.batch_size]
            batch_number = (i // self.batch_size) + 1
            total_batches = (total_records + self.batch_size - 1) // self.batch_size
            
            logging.info(f" Batch {batch_number}/{total_batches}: {len(batch)} records")
            
            # Send the batch
           # print(f"[DEBUG] SEND batch {batch}")
            response = send_method(batch, schema_type, field_id, version) if operation_type == "new" else send_method(batch, schema_type, field_id)
            print(f"[DEBUG] SEND RESPONSE {response}")
            
            # Check response
            if response and response.get('status') == 'ok':
                successful_batches += 1
                logging.info(f"  Batch {batch_number} completed: {response.get('status_id')}")
            else:
                error_msg = response.get('msg', 'Unknown error') if response else 'No response'
                logging.warning(f" Batch {batch_number} failed: {error_msg}")
            
            # Track the response
            if self.sql_enabled and track_method and response:
                if operation_type == "new":
                    track_method(response, batch, field_id, version)
                else:
                    track_method(response, batch, field_id, version)
        
        logging.info(f"{operation_type.capitalize()} records: {successful_batches}/{total_batches} batches successful")