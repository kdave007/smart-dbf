from ..dbf_enc_reader.mapping_manager import MappingManager
from pathlib import Path
import hashlib
import json

class Composed:
    def __init__(self, mapping_manager: MappingManager, config: DBFConfig):
        self.mapping_manager = mapping_manager

  
