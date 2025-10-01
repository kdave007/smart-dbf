import hashlib
import json

class HashManager:

    def hash_record(self, raw_record):
        """Generate MD5 hash for a record"""
        # Convert record to JSON string for consistent hashing
        record_string = json.dumps(raw_record, sort_keys=True, default=str)
        
        # Generate MD5 hash
        md5_hash = hashlib.md5(record_string.encode('utf-8')).hexdigest()
        
        return md5_hash