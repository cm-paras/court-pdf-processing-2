#!/usr/bin/env python3
"""Clear only Azure Cognitive Search index (not Cosmos DB)."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging_config import setup_logging
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
import logging

def clear_search_index_only():
    """Clear only Azure Cognitive Search index."""
    setup_logging("INFO")
    load_dotenv()
    
    try:
        # Direct search client (no Cosmos DB involved)
        search_client = SearchClient(
            endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
            index_name=os.getenv("AZURE_SEARCH_INDEX_NAME"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))
        )
        
        logging.info("Clearing Azure Cognitive Search index only...")
        
        # Get document count first
        count_result = search_client.search("*", include_total_count=True, top=0)
        total_docs = count_result.get_count()
        logging.info(f"Found {total_docs} documents in search index")
        
        if total_docs == 0:
            logging.info("Search index is already empty")
            return True
        
        # Get all document IDs in batches
        batch_size = 1000
        total_deleted = 0
        
        while True:
            # Get next batch of IDs
            results = search_client.search("*", select=["id"], top=batch_size)
            ids_to_delete = [{"id": result["id"]} for result in results]
            
            if not ids_to_delete:
                break
                
            # Delete in smaller batches of 100
            delete_batch_size = 100
            for i in range(0, len(ids_to_delete), delete_batch_size):
                batch = ids_to_delete[i:i + delete_batch_size]
                delete_result = search_client.delete_documents(batch)
                successful_deletes = sum(1 for item in delete_result if item.succeeded)
                total_deleted += successful_deletes
                logging.info(f"Deleted {successful_deletes}/{len(batch)} documents (Total: {total_deleted})")
        
        logging.info(f"Search index cleared. Total documents deleted: {total_deleted}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to clear search index: {e}")
        return False

if __name__ == "__main__":
    success = clear_search_index_only()
    sys.exit(0 if success else 1)