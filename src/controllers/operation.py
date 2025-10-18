import logging
import requests
from ..utils.response_simulator import ResponseSimulator
import json
import sys

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
            'x-api-key': str(api_key),
            'Content-Type': 'application/json'  # Por defecto JSON
        }
        self.ndjson_headers = {
            'x-api-key': str(api_key),
            'Content-Type': 'text/plain'  # Para NDJSON
        }
    
    def send_new_records(self, new_records, schema, field_id, version):
        """Envía registros nuevos a endpoint específico"""
        if not new_records:
            print("No hay registros nuevos para enviar")
            return
    
        # Convertir a NDJSON
        ndjson_data = "\n".join(json.dumps(record, separators=(',', ':')) for record in new_records)
        
        # print(f" NDJSON SEND NEW: {(ndjson_data)}")
        logging.info(f"-- Post request  : {len(new_records)} registros en formato NDJSON")

        if self.simulate_response:
            logging.info(f"  [ RESPONSE SIMULATION ] ")
            server_response = self.response_simulator.simulate_api_response(new_records, field_id, 'create', schema)
        else:
            server_response = self._send_request_ndjson(
                self.endpoints['new'], 
                ndjson_data, 
                'create', 
                schema, 
                field_id, 
                version,
                len(new_records)
            )

        return server_response
    
    def send_updates(self, changed_records, schema, field_id):
        """Envía actualizaciones a endpoint específico"""
        if not changed_records:
            print("No hay registros para actualizar")
            return
            
        # Convertir a NDJSON
        ndjson_data = "\n".join(json.dumps(record, separators=(',', ':')) for record in changed_records)
        
        logging.info(f"-- Post request : {len(changed_records)} registros en formato NDJSON")
        print(f" UDPATE ____ { changed_records }")
        

        if self.simulate_response:
            logging.info(f"  [ RESPONSE SIMULATION ] ")
            server_response = self.response_simulator.simulate_api_response(changed_records, field_id, 'update', schema)
        else:
            server_response = self._send_request_ndjson(
                self.endpoints['update'], 
                ndjson_data, 
                'update', 
                schema, 
                field_id, 
                None,
                len(changed_records)
            )

        return server_response
    
    def send_deletes(self, deleted_records, schema, field_id):
        """Envía eliminaciones a endpoint específico"""
        if not deleted_records:
            print("No hay registros para eliminar")
            return
            
        # Convertir a NDJSON
        ndjson_data = "\n".join(json.dumps(record, separators=(',', ':')) for record in deleted_records)
        
        logging.info(f"-- Post request : {len(deleted_records)} registros en formato NDJSON")

        if self.simulate_response:
            logging.info(f"  [ RESPONSE SIMULATION ] ")
            server_response = self.response_simulator.simulate_api_response(deleted_records, field_id, 'delete', schema)
        else:
            server_response = self._send_request_ndjson(
                self.endpoints['delete'], 
                ndjson_data, 
                'delete', 
                schema, 
                field_id, 
                None,
                len(deleted_records)
            )

        return server_response
    
    def _send_request_ndjson(self, url, ndjson_data, operation, schema, field_id, version, count):
        """Método interno para enviar NDJSON"""
        try:
            # Parámetros para metadata
            params = {
                'operation': operation,
                'schema': schema,
                'field_id': field_id,
                'table_name': self.table_name,
                'client_id': self.client_id,
                'count': count
            }
            
            # Añadir version si existe
            if version:
                params['ver'] = version
            
            # Enviar como NDJSON (text/plain)
            response = requests.post(
                url,
                data=ndjson_data,
                headers=self.ndjson_headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            logging.info(f" Enviados {count} registros a {url} (NDJSON)")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error(f" Error enviando NDJSON a {url}: {e}")
            return None
    
    def _send_request(self, url, payload):
        """Método original para compatibilidad (puedes eliminarlo luego)"""
        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()
            print(f"Enviados {payload['count']} registros a {url}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error enviando a {url}: {e}")
            return None