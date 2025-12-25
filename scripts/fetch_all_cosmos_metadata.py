"""
Fetch all metadata documents from Azure Cosmos DB and save them locally as JSON and CSV.
"""

import os
import json
import csv
from tqdm import tqdm

import sys
from pathlib import Path

# Ensure project root is in sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.storage.cosmos_storage import CosmosStorage
from src.clients.azure_clients import AzureClientManager
from src.config.settings import AZURE_CONFIG


def fetch_all_cosmos_metadata(output_prefix="all_cosmos_metadata"):
    """Fetch all metadata documents from Cosmos DB and save as JSON and CSV."""
    print("Connecting to Azure Cosmos DB...")
    # Patch config keys for backward compatibility
    class PatchedConfig:
        def __init__(self, cfg):
            self.AZURE_STORAGE_CONNECTION_STRING = cfg.STORAGE_CONNECTION_STRING
            self.BLOB_CONTAINER_NAME = cfg.BLOB_CONTAINER_NAME
            self.COSMOS_DB_ENDPOINT = cfg.COSMOS_ENDPOINT
            self.COSMOS_DB_KEY = cfg.COSMOS_KEY
            self.COSMOS_DB_DATABASE = cfg.COSMOS_DATABASE
            self.COSMOS_DB_CONTAINER = cfg.COSMOS_CONTAINER
            self.AZURE_SEARCH_ENDPOINT = cfg.SEARCH_ENDPOINT
            self.AZURE_SEARCH_KEY = cfg.SEARCH_KEY
            self.AZURE_SEARCH_INDEX_NAME = cfg.SEARCH_INDEX_NAME
            self.AZURE_OPENAI_API_KEY = cfg.OPENAI_API_KEY
            self.AZURE_OPENAI_ENDPOINT = cfg.OPENAI_ENDPOINT
            self.GEMINI_API_KEY = cfg.GEMINI_API_KEY
            self.GEMINI_MODEL = cfg.GEMINI_MODEL

    # Add retry and delay defaults
    patched_config = PatchedConfig(AZURE_CONFIG)
    patched_config.MAX_RETRIES = 5
    patched_config.RETRY_DELAY = 2.0
    patched_config.AZURE_OPENAI_CHAT_MODEL = "gpt-4o-mini"  # dummy placeholder to satisfy AzureClientManager

    # Initialize only Cosmos client directly to avoid unnecessary OpenAI setup
    from azure.cosmos import CosmosClient
    cosmos_client = CosmosClient(
        url=patched_config.COSMOS_DB_ENDPOINT,
        credential=patched_config.COSMOS_DB_KEY
    )
    database = cosmos_client.get_database_client(patched_config.COSMOS_DB_DATABASE)
    container = database.get_container_client(patched_config.COSMOS_DB_CONTAINER)

    print("Fetching all documents from Cosmos DB...")
    all_docs = []
    for item in container.query_items(query="SELECT * FROM c", enable_cross_partition_query=True):
        all_docs.append(item)
    total_docs = len(all_docs)
    print(f"✅ Retrieved {total_docs} documents from Cosmos DB")

    if not all_docs:
        print("⚠️ No documents found in Cosmos DB.")
        return

    # Save as JSON
    output_prefix = "all_metadata"
    json_output = f"{output_prefix}.json"
    with open(json_output, "w", encoding="utf-8") as f_json:
        json.dump(all_docs, f_json, ensure_ascii=False, indent=2)
    print(f"✅ Saved all metadata as JSON to {json_output}")

    # Save as CSV
    csv_output = f"{output_prefix}.csv"
    keys = sorted({k for doc in all_docs for k in doc.keys()})
    with open(csv_output, "w", newline="", encoding="utf-8") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_docs)
    print(f"✅ Saved all metadata as CSV to {csv_output}")

    print(f"\n✅ Completed fetching and saving {total_docs} metadata documents.")


if __name__ == "__main__":
    fetch_all_cosmos_metadata()
