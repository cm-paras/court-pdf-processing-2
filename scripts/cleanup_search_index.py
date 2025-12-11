"""Cleanup script to remove junk data from Azure Search index."""

import logging
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
import os

load_dotenv()

def cleanup_search_index():
    """Remove junk chunks from Azure Search index."""
    
    # Initialize search client
    client = SearchClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        index_name=os.getenv("AZURE_SEARCH_INDEX_NAME"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))
    )
    
    logging.info("Starting search index cleanup...")
    
    # Query for junk documents
    junk_queries = [
        "content eq 'No text content could be extracted'",
        "content eq ''",
        "case_name eq '' and case_number eq '' and court eq ''"
    ]
    
    total_deleted = 0
    
    for query in junk_queries:
        try:
            # Search for junk documents
            results = client.search(
                search_text="*",
                filter=query,
                select=["id"],
                top=1000
            )
            
            # Collect IDs to delete
            ids_to_delete = []
            for result in results:
                ids_to_delete.append({"id": result["id"]})
            
            if ids_to_delete:
                # Delete in batches
                batch_size = 100
                for i in range(0, len(ids_to_delete), batch_size):
                    batch = ids_to_delete[i:i + batch_size]
                    delete_result = client.delete_documents(batch)
                    
                    successful_deletes = sum(1 for item in delete_result if item.succeeded)
                    total_deleted += successful_deletes
                    
                    logging.info(f"Deleted {successful_deletes}/{len(batch)} documents in batch")
            
            logging.info(f"Query '{query}': Found and deleted {len(ids_to_delete)} documents")
            
        except Exception as e:
            logging.error(f"Error processing query '{query}': {e}")
    
    logging.info(f"Cleanup complete. Total documents deleted: {total_deleted}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cleanup_search_index()