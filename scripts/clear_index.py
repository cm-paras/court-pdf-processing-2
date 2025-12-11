#!/usr/bin/env python3
"""Clear all documents from Azure Search index."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging_config import setup_logging
from src.clients.azure_clients import AzureClientManager
from src.config.config import Config
import logging

def clear_index():
    """Clear all documents from the search index."""
    setup_logging("INFO")
    
    try:
        config = Config()
        config.validate()
        client_manager = AzureClientManager(config)
        search_client = client_manager.search_client
        
        logging.info("Clearing search index...")
        
        # Get all document IDs
        results = search_client.search("*", select=["id"], top=10000)
        ids_to_delete = [{"id": result["id"]} for result in results]
        
        if ids_to_delete:
            # Delete in batches
            batch_size = 100
            total_deleted = 0
            
            for i in range(0, len(ids_to_delete), batch_size):
                batch = ids_to_delete[i:i + batch_size]
                delete_result = search_client.delete_documents(batch)
                successful_deletes = sum(1 for item in delete_result if item.succeeded)
                total_deleted += successful_deletes
                logging.info(f"Deleted {successful_deletes}/{len(batch)} documents")
            
            logging.info(f"Index cleared. Total documents deleted: {total_deleted}")
        else:
            logging.info("Index is already empty")
            
    except Exception as e:
        logging.error(f"Failed to clear index: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = clear_index()
    sys.exit(0 if success else 1)