import requests
from ..utils.response_simulator import ResponseSimulator

class Operation:
    def __init__(self, config, table_name, client_id, simulate_response=False):
        
        self.simulate_response = simulate_response
        self.response_simulator = ResponseSimulator()
        self.table_name = table_name
        self.client_id = client_id

        base_url = config.get('POST_API_BASE')    
        create = config.get('CREATE')
        update = config.get('UPDATE')
        delete = config.get('DELETE')
        
        self.endpoints = {
            'new': f"{base_url}/{create}",
            'update': f"{base_url}/{update}", 
            'delete': f"{base_url}/{delete}"
        }
        
        api_key = config.get('testing_api_key')
        self.headers = {
            'x-api-key': str(api_key)
        }
    
    def send_new_records(self, new_records, schema, field_id, version):
        """Envía registros nuevos a endpoint específico"""
        if not new_records:
            print("No hay registros nuevos para enviar")
            return
    
            
        payload = {
            'operation': 'create',
            'records': new_records,
            'count': len(new_records),
            'schema': schema,
            'field_id': field_id,
            'table_name': self.table_name,
            "client_id":self.client_id,
            "ver": version
        }

        print(f"SEND NEW : ",payload)

        if self.simulate_response:
            print(f"//////  RESPONSE SIMULATION ///////")
            server_response = self.response_simulator.simulate_api_response(new_records, field_id, 'create', schema)
        else:
            server_response = self._send_request(self.endpoints['new'], payload)

        return server_response
    
    def send_updates(self, changed_records, schema, field_id):
        """Envía actualizaciones a endpoint específico"""
        if not changed_records:
            print("No hay registros para actualizar")
            return
            
        payload = {
            'operation': 'update', 
            'records': changed_records,
            'count': len(changed_records),
            'schema': schema,
            'field_id': field_id,
            'table_name': self.table_name,
            "client_id":self.client_id,
        }
        
        if self.simulate_response:
            print(f"//////  RESPONSE SIMULATION ///////")
            server_response = self.response_simulator.simulate_api_response(changed_records, field_id, 'update', schema)
        else:
            server_response = self._send_request(self.endpoints['update'], payload)

        return server_response
    
    def send_deletes(self, deleted_records, schema, field_id):
        """Envía eliminaciones a endpoint específico"""
        if not deleted_records:
            print("No hay registros para eliminar")
            return
            
        payload = {
            'operation': 'delete',
            'records': deleted_records, 
            'count': len(deleted_records),
            'schema': schema,
            'field_id': field_id,
            'table_name': self.table_name
        }
        
        if self.simulate_response:
            print(f"//////  RESPONSE SIMULATION ///////")
            server_response = self.response_simulator.simulate_api_response(deleted_records, field_id, 'delete', schema)
        else:
            server_response = self._send_request(self.endpoints['delete'], payload)

        return server_response
    
    def _send_request(self, url, payload):
        """Método interno para enviar requests"""
        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()
            print(f"Enviados {payload['count']} registros a {url}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error enviando a {url}: {e}")
            return None