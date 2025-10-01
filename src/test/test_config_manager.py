#!/usr/bin/env python3
"""
Test script for ConfigManager to demonstrate .env file loading
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_manager import ConfigManager

def test_config_manager():
    """Test the ConfigManager with .env file"""
    
    print("Testing ConfigManager with .env file...")
    print("-" * 50)
    
    try:
        # Initialize ConfigManager with ENV source
        config_manager = ConfigManager("ENV")
        
        # Load parameters from .env
        params = config_manager.get_params()
        
        print("All loaded parameters:")
        for key, value in params.items():
            print(f"  {key}: {value} (type: {type(value).__name__})")
        
        print("\n" + "-" * 50)
        
        # Test specific value retrieval
        print("Testing specific value retrieval:")
        stop_flag = config_manager.get_config_value('STOP_FLAG')
        chunks_size = config_manager.get_config_value('DBF_CHUNKS_SIZE')
        base_url = config_manager.get_config_value('POST_API_BASE')
        
        print(f"  STOP_FLAG: {stop_flag}")
        print(f"  DBF_CHUNKS_SIZE: {chunks_size}")
        print(f"  POST_API_BASE: {base_url}")
        
        print("\n" + "-" * 50)
        
        # Test API endpoints configuration
        print("API Endpoints:")
        endpoints = config_manager.get_api_endpoints()
        for name, url in endpoints.items():
            print(f"  {name}: {url}")
        
        print("\n" + "-" * 50)
        
        # Test processing configuration
        print("Processing Configuration:")
        processing_config = config_manager.get_processing_config()
        for key, value in processing_config.items():
            print(f"  {key}: {value}")
        
        print("\nConfigManager test completed successfully!")
        
    except Exception as e:
        print(f"Error testing ConfigManager: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_config_manager()
