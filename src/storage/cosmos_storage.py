"""
Azure Cosmos DB storage functionality
"""
import base64
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.cosmos import exceptions

import logging

logger = logging.getLogger(__name__)


class CosmosStorage:
    """Handles document storage in Azure Cosmos DB"""
    
    def __init__(self, azure_clients, config):
        """
        Initialize Cosmos storage
        
        Args:
            azure_clients: AzureClientManager instance
            config: Configuration object
        """
        self.container = azure_clients.cosmos_container
        self.config = config
    
    def documents_exist_batch(self, blob_names):
        """
        Check which documents already exist in Cosmos DB (batch operation)
        
        Args:
            blob_names: List of blob names to check
            
        Returns:
            set: Set of blob names that exist
        """
        try:
            if not blob_names:
                return set()
            
            # Create IN clause for batch query
            blob_names_str = ", ".join([f"'{name}'" for name in blob_names[:100]])  # Limit to 100
            query = f"SELECT c.blob_name FROM c WHERE c.blob_name IN ({blob_names_str})"
            
            results = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            return {result['blob_name'] for result in results}
        except Exception as e:
            logger.warning(f"Error checking batch document existence: {e}")
            return set()
    
    def document_exists(self, blob_name):
        """
        Check if document already exists in Cosmos DB using query
        
        Args:
            blob_name: Name of the blob to check
            
        Returns:
            bool: True if document exists
        """
        existing = self.documents_exist_batch([blob_name])
        return blob_name in existing
    
    def store_document(self, blob_name, metadata, text_sample=None):
        """
        Store metadata in Cosmos DB
        
        Args:
            blob_name: Name of the blob
            metadata: Document metadata
            text_sample: Optional text sample
            
        Returns:
            bool: Success status
        """
        if not metadata:
            logger.warning(f"Attempted to store None metadata for {blob_name}")
            return False
        
        try:
            doc_id = base64.b64encode(blob_name.encode('utf-8')).decode('utf-8')
            
            # Extract year and court from metadata or use defaults
            year = metadata.get("Date of Judgment", "2024")[:4] if metadata.get("Date of Judgment") else "2024"
            court = metadata.get("Court", "HighCourt") or "HighCourt"
            
            document = {
                "id": doc_id,
                "blob_name": blob_name,
                "year": year,
                "court": court,
                "metadata": metadata,
                "text_sample": text_sample[:1000] if text_sample else None
            }
            
            for attempt in range(self.config.MAX_RETRIES):
                try:
                    self.container.upsert_item(document)
                    logger.info(f"Stored metadata for {blob_name} in Cosmos DB")
                    return True
                except exceptions.CosmosHttpResponseError as e:
                    if attempt < self.config.MAX_RETRIES - 1:
                        logger.warning(
                            f"Cosmos DB error on attempt {attempt+1}/{self.config.MAX_RETRIES}: {str(e)}"
                        )
                        time.sleep(self.config.RETRY_DELAY * (2 ** attempt))
                    else:
                        logger.error(
                            f"Final Cosmos DB error after {self.config.MAX_RETRIES} attempts: {str(e)}"
                        )
                        return False
                except Exception as e:
                    logger.error(f"Unexpected error storing document in Cosmos DB: {str(e)}")
                    return False
            
            return False
        except Exception as e:
            logger.error(f"Error in store_document for {blob_name}: {str(e)}")
            return False
    
    def store_batch(self, metadata_dict, text_samples=None):
        """
        Store multiple documents in Cosmos DB
        
        Args:
            metadata_dict: Dictionary mapping blob names to metadata
            text_samples: Optional dictionary mapping blob names to text samples
            
        Returns:
            tuple: (successful_set, failed_set)
        """
        successful = set()
        failed = set()
        
        operations = []
        
        for blob_name, metadata in metadata_dict.items():
            if not metadata:
                failed.add(blob_name)
                continue
            
            # Extract year and court from metadata or use defaults
            year = metadata.get("Date of Judgment", "2024")[:4] if metadata.get("Date of Judgment") else "2024"
            court = metadata.get("Court", "HighCourt") or "HighCourt"
            
            document = {
                "id": base64.b64encode(blob_name.encode('utf-8')).decode('utf-8'),
                "blob_name": blob_name,
                "year": year,
                "court": court,
                "metadata": metadata,
                "text_sample": text_samples.get(blob_name, "")[:1000] if text_samples else None
            }
            
            operations.append(document)
            
            if len(operations) >= self.config.COSMOS_BATCH_SIZE:
                success, fail = self._execute_batch(operations)
                successful.update(success)
                failed.update(fail)
                operations = []
        
        if operations:
            success, fail = self._execute_batch(operations)
            successful.update(success)
            failed.update(fail)
        
        return successful, failed
    
    def _execute_batch(self, documents):
        """Execute a batch operation in Cosmos DB"""
        successful = set()
        failed = set()
        
        try:
            with ThreadPoolExecutor(max_workers=min(len(documents), 50)) as executor:
                futures = []
                
                for doc in documents:
                    blob_name = doc.get("blob_name")
                    future = executor.submit(self._store_single, doc)
                    futures.append((future, blob_name))
                
                for future, blob_name in futures:
                    try:
                        if future.result():
                            successful.add(blob_name)
                        else:
                            failed.add(blob_name)
                    except Exception as e:
                        logger.error(f"Error storing document for {blob_name}: {e}")
                        failed.add(blob_name)
        
        except Exception as e:
            logger.error(f"Error executing Cosmos DB batch: {e}")
            for doc in documents:
                failed.add(doc.get("blob_name"))
        
        return successful, failed
    
    def _store_single(self, document):
        """Store a single document with retries"""
        for attempt in range(self.config.MAX_RETRIES):
            try:
                self.container.upsert_item(document)
                return True
            except exceptions.CosmosHttpResponseError as e:
                if attempt < self.config.MAX_RETRIES - 1:
                    time.sleep(self.config.RETRY_DELAY * (2 ** attempt))
                else:
                    logger.error(f"Cosmos DB error after {self.config.MAX_RETRIES} attempts: {e}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error storing document: {e}")
                return False
    
    def query_all_documents(self, max_items=1000):
        """
        Query all documents from Cosmos DB with limit
        
        Args:
            max_items: Maximum number of items to return
            
        Returns:
            list: Documents from the container (limited)
        """
        try:
            query = "SELECT * FROM c"
            documents = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True,
                max_item_count=max_items
            ))
            return documents
        except Exception as e:
            logger.error(f"Error querying documents from Cosmos DB: {e}")
            return []
