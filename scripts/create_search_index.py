"""Script to create Azure Search index with proper schema."""

import json
import logging
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
import os

load_dotenv()

def create_search_index():
    """Create Azure Search index from schema."""
    
    # Initialize client
    client = SearchIndexClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))
    )
    
    # Load schema
    with open("search_index_schema.json", "r") as f:
        schema = json.load(f)
    
    try:
        # Create index
        index = SearchIndex.from_dict(schema)
        result = client.create_or_update_index(index)
        
        logging.info(f"Successfully created/updated index: {result.name}")
        
    except Exception as e:
        logging.error(f"Failed to create index: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_search_index()