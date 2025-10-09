import random

class ResponseSimulator:
    def __init__(self) -> None:
        pass
    
    def simulate_api_response(self, records, field_name, operation, schema):
        """Simulate API response with field values and status"""
        if not records:
            return {"status": "error", "msg": "empty list", "status_code": 500}
        
        # Generate a random id_cola (same for all records in this batch)
        id_cola = random.randint(10000, 99999)
        
        return {
            "status": "ok",
            "id_cola": id_cola,
            "status_id":1,
            "status_code": 200
        }