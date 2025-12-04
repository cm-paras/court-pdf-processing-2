#!/usr/bin/env python3
"""
PDF Processing Pipeline Cleaner

This script provides utilities to clean different components:
- Clear all metadata from Cosmos DB
- Clear all documents from Azure Cognitive Search index
- Delete and recreate search index
- Clean temporary files and caches
"""

import sys
import os
import argparse
import time
from datetime import datetime

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config import Config
from src.clients import AzureClientManager
from src.storage import CosmosStorage, SearchIndexer
from src.utils import configure_logging

logger = configure_logging()


class PipelineCleaner:
    """Handles cleanup of pipeline components"""
    
    def __init__(self):
        """Initialize the cleaner"""
        try:
            self.config = Config()
            self.config.validate()
            self.azure_clients = AzureClientManager(self.config)
            self.cosmos_storage = CosmosStorage(self.azure_clients, self.config)
            self.search_indexer = SearchIndexer(self.azure_clients, self.config)
            
            print("✅ Successfully connected to Azure services")
        except Exception as e:
            print(f"❌ Error initializing cleaner: {e}")
            raise
    
    def clean_cosmos_metadata(self, confirm=True):
        """
        Delete all metadata documents from Cosmos DB
        
        Args:
            confirm: Whether to ask for confirmation
        """
        print("\n" + "="*60)
        print("🗑️  CLEANING COSMOS DB METADATA")
        print("="*60)
        
        if confirm:
            response = input("⚠️  This will DELETE ALL metadata from Cosmos DB. Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("❌ Operation cancelled")
                return False
        
        try:
            # Get all documents
            print("📋 Retrieving all documents from Cosmos DB...")
            all_documents = self.cosmos_storage.query_all_documents()
            
            if not all_documents:
                print("ℹ️  No documents found in Cosmos DB")
                return True
            
            print(f"📊 Found {len(all_documents)} documents to delete")
            
            # Delete documents in batches
            deleted_count = 0
            failed_count = 0
            batch_size = 100
            
            for i in range(0, len(all_documents), batch_size):
                batch = all_documents[i:i + batch_size]
                print(f"🔄 Deleting batch {i//batch_size + 1}/{(len(all_documents) + batch_size - 1)//batch_size}")
                
                for doc in batch:
                    try:
                        doc_id = doc.get('id')
                        if doc_id:
                            self.cosmos_storage.container.delete_item(
                                item=doc_id,
                                partition_key=doc_id
                            )
                            deleted_count += 1
                        else:
                            print(f"⚠️  Document missing ID: {doc.get('blob_name', 'Unknown')}")
                            failed_count += 1
                    except Exception as e:
                        print(f"❌ Error deleting document {doc.get('blob_name', 'Unknown')}: {e}")
                        failed_count += 1
                
                # Brief pause between batches
                if i + batch_size < len(all_documents):
                    time.sleep(0.1)
            
            print(f"\n✅ Cleanup completed!")
            print(f"   📊 Deleted: {deleted_count} documents")
            print(f"   ❌ Failed: {failed_count} documents")
            
            return failed_count == 0
            
        except Exception as e:
            print(f"❌ Error cleaning Cosmos DB: {e}")
            return False
    
    def clean_search_index(self, confirm=True):
        """
        Delete all documents from Azure Cognitive Search index
        
        Args:
            confirm: Whether to ask for confirmation
        """
        print("\n" + "="*60)
        print("🗑️  CLEANING AZURE COGNITIVE SEARCH INDEX")
        print("="*60)
        
        if confirm:
            response = input("⚠️  This will DELETE ALL indexed documents. Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("❌ Operation cancelled")
                return False
        
        try:
            search_client = self.azure_clients.search_client
            
            # Get all document IDs
            print("📋 Retrieving all document IDs from search index...")
            results = search_client.search(
                search_text="*",
                select="id",
                top=50000  # Adjust based on your index size
            )
            
            document_ids = []
            for result in results:
                document_ids.append(result['id'])
            
            if not document_ids:
                print("ℹ️  No documents found in search index")
                return True
            
            print(f"📊 Found {len(document_ids)} documents to delete")
            
            # Delete in batches
            deleted_count = 0
            failed_count = 0
            batch_size = 1000  # Azure Search can handle larger batches for deletions
            
            for i in range(0, len(document_ids), batch_size):
                batch_ids = document_ids[i:i + batch_size]
                print(f"🔄 Deleting batch {i//batch_size + 1}/{(len(document_ids) + batch_size - 1)//batch_size}")
                
                # Prepare batch deletion documents
                delete_docs = [{"@search.action": "delete", "id": doc_id} for doc_id in batch_ids]
                
                try:
                    result = search_client.upload_documents(delete_docs)
                    
                    # Count successes and failures
                    for item in result:
                        if item.succeeded:
                            deleted_count += 1
                        else:
                            failed_count += 1
                            print(f"❌ Failed to delete document: {item.key}")
                    
                except Exception as e:
                    print(f"❌ Error deleting batch: {e}")
                    failed_count += len(batch_ids)
                
                # Brief pause between batches
                if i + batch_size < len(document_ids):
                    time.sleep(0.5)
            
            print(f"\n✅ Search index cleanup completed!")
            print(f"   📊 Deleted: {deleted_count} documents")
            print(f"   ❌ Failed: {failed_count} documents")
            
            return failed_count == 0
            
        except Exception as e:
            print(f"❌ Error cleaning search index: {e}")
            return False
    
    def recreate_search_index(self, confirm=True):
        """
        Delete and recreate the search index (complete reset)
        
        Args:
            confirm: Whether to ask for confirmation
        """
        print("\n" + "="*60)
        print("🔄 RECREATING AZURE COGNITIVE SEARCH INDEX")
        print("="*60)
        
        if confirm:
            response = input("⚠️  This will DELETE and RECREATE the entire search index. Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("❌ Operation cancelled")
                return False
        
        try:
            import requests
            
            index_name = self.config.AZURE_SEARCH_INDEX_NAME
            base_url = f"{self.config.AZURE_SEARCH_ENDPOINT}/indexes/{index_name}"
            headers = {
                "Content-Type": "application/json",
                "api-key": self.config.AZURE_SEARCH_KEY
            }
            
            # Delete existing index
            print("🗑️  Deleting existing index...")
            delete_url = f"{base_url}?api-version=2023-10-01-Preview"
            delete_response = requests.delete(delete_url, headers=headers)
            
            if delete_response.status_code in (200, 204, 404):
                print("✅ Existing index deleted (or didn't exist)")
            else:
                print(f"⚠️  Warning: Delete response: {delete_response.status_code}")
            
            # Wait a moment for deletion to complete
            time.sleep(2)
            
            # Recreate index
            print("🔄 Creating new index...")
            success = self.search_indexer.create_index()
            
            if success:
                print("✅ Search index recreated successfully!")
                return True
            else:
                print("❌ Failed to recreate search index")
                return False
            
        except Exception as e:
            print(f"❌ Error recreating search index: {e}")
            return False
    
    def clean_temporary_files(self):
        """Clean temporary files and caches"""
        print("\n" + "="*60)
        print("🧹 CLEANING TEMPORARY FILES")
        print("="*60)
        
        temp_dirs = [
            "/tmp",
            "/tmp/pdf_processing",
            "./temp",
            "./downloads",
            "./__pycache__",
            "./src/__pycache__"
        ]
        
        cleaned_files = 0
        
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                try:
                    if temp_dir.endswith("__pycache__"):
                        # Clean Python cache files
                        for root, dirs, files in os.walk(temp_dir):
                            for file in files:
                                if file.endswith('.pyc'):
                                    file_path = os.path.join(root, file)
                                    os.remove(file_path)
                                    cleaned_files += 1
                    else:
                        # Clean PDF temp files
                        for file in os.listdir(temp_dir):
                            if file.endswith('.pdf') or file.startswith('temp_'):
                                file_path = os.path.join(temp_dir, file)
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                                    cleaned_files += 1
                                    
                except Exception as e:
                    print(f"⚠️  Error cleaning {temp_dir}: {e}")
        
        print(f"✅ Cleaned {cleaned_files} temporary files")
        return True
    
    def full_cleanup(self, confirm=True):
        """
        Perform complete cleanup of all components
        
        Args:
            confirm: Whether to ask for confirmation
        """
        print("\n" + "="*80)
        print("🚨 FULL PIPELINE CLEANUP")
        print("="*80)
        
        if confirm:
            print("This will:")
            print("• Delete ALL metadata from Cosmos DB")
            print("• Delete and recreate the Azure Cognitive Search index")
            print("• Clean temporary files")
            print()
            response = input("⚠️  Are you absolutely sure? (yes/no): ")
            if response.lower() != 'yes':
                print("❌ Operation cancelled")
                return False
        
        success = True
        
        # Clean Cosmos DB
        if not self.clean_cosmos_metadata(confirm=False):
            success = False
        
        # Recreate Search Index
        if not self.recreate_search_index(confirm=False):
            success = False
        
        # Clean temporary files
        if not self.clean_temporary_files():
            success = False
        
        if success:
            print("\n" + "="*60)
            print("🎉 FULL CLEANUP COMPLETED SUCCESSFULLY!")
            print("="*60)
            print("Pipeline is now completely clean and ready for fresh processing.")
        else:
            print("\n" + "="*60)
            print("⚠️  CLEANUP COMPLETED WITH SOME ERRORS")
            print("="*60)
            print("Check the logs above for details.")
        
        return success
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.azure_clients.cleanup()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Clean PDF Processing Pipeline Components")
    
    # Cleaning options
    parser.add_argument("--cosmos", action="store_true", 
                       help="Clean all metadata from Cosmos DB")
    parser.add_argument("--search", action="store_true", 
                       help="Clean all documents from search index")
    parser.add_argument("--recreate-index", action="store_true", 
                       help="Delete and recreate search index")
    parser.add_argument("--temp-files", action="store_true", 
                       help="Clean temporary files")
    parser.add_argument("--full", action="store_true", 
                       help="Perform complete cleanup (all components)")
    
    # Safety options
    parser.add_argument("--force", action="store_true", 
                       help="Skip confirmation prompts (DANGEROUS)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be cleaned without actually doing it")
    
    args = parser.parse_args()
    
    # Check if any action is specified
    if not any([args.cosmos, args.search, args.recreate_index, args.temp_files, args.full]):
        parser.print_help()
        print("\n❌ Please specify at least one cleanup action.")
        return 1
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No actual cleanup will be performed")
        print("-" * 60)
        if args.cosmos or args.full:
            print("• Would clean Cosmos DB metadata")
        if args.search:
            print("• Would clean search index documents")
        if args.recreate_index or args.full:
            print("• Would recreate search index")
        if args.temp_files or args.full:
            print("• Would clean temporary files")
        return 0
    
    cleaner = None
    try:
        cleaner = PipelineCleaner()
        confirm = not args.force
        
        success = True
        
        if args.full:
            success = cleaner.full_cleanup(confirm=confirm)
        else:
            if args.cosmos:
                if not cleaner.clean_cosmos_metadata(confirm=confirm):
                    success = False
            
            if args.search:
                if not cleaner.clean_search_index(confirm=confirm):
                    success = False
            
            if args.recreate_index:
                if not cleaner.recreate_search_index(confirm=confirm):
                    success = False
            
            if args.temp_files:
                if not cleaner.clean_temporary_files():
                    success = False
        
        if success:
            print("\n🎉 All requested cleanup operations completed successfully!")
        else:
            print("\n⚠️  Some cleanup operations failed. Check logs for details.")
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n❌ Cleanup interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        return 1
    finally:
        if cleaner:
            cleaner.cleanup()


if __name__ == "__main__":
    sys.exit(main())
