#!/usr/bin/env python3
"""Delete and recreate Azure Search index (much faster than clearing)."""

import sys
import os
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
import logging

def recreate_index():
    """Delete and recreate the search index."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    load_dotenv()
    
    try:
        # Initialize search index client
        index_client = SearchIndexClient(
            endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))
        )
        
        index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")
        
        # Delete existing index
        logging.info(f"Deleting index: {index_name}")
        try:
            index_client.delete_index(index_name)
            logging.info("Index deleted successfully")
        except Exception as e:
            logging.warning(f"Index deletion failed (may not exist): {e}")
        
        # Load schema and create new index
        schema_path = os.path.join(os.path.dirname(__file__), "search_index_schema.json")
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        # Update index name in schema
        schema["name"] = index_name
        
        logging.info(f"Creating new index: {index_name}")
        index = SearchIndex.from_dict(schema)
        index_client.create_index(index)
        
        logging.info("Index recreated successfully!")
        return True
        
    except Exception as e:
        logging.error(f"Failed to recreate index: {e}")
        return False

if __name__ == "__main__":
    success = recreate_index()
    sys.exit(0 if success else 1)