import random

class ResponseSimulator:
    def __init__(self) -> None:
        pass
    
    def simulate_api_response(self, records, field_name, operation, schema):
        """Simulate API response with field values and status"""
        if not records:
            return {"status": "ok", "records": []}
        
        # Generate a random id_cola (same for all records in this batch)
        id_cola = random.randint(10000, 99999)
        
        response_records = []
        for record in records:
            # Get the field value from __meta
            field_value = record.get('__meta', {}).get(field_name, 'unknown')
            
            # Create response record
            response_record = {
                field_name: field_value,
                "status": 2,
                "desc": "pending",
                "id_cola": id_cola
            }
            response_records.append(response_record)
        
        return {
            "status": "ok",
            "records": response_records,
            "operation":operation,
            "schema":schema
        }