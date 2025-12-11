"""
Azure Cognitive Search indexing functionality
"""
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging

logger = logging.getLogger(__name__)


class SearchIndexer:
    """Handles Azure Cognitive Search index management and document upload"""
    
    def __init__(self, azure_clients, config):
        """
        Initialize search indexer
        
        Args:
            azure_clients: AzureClientManager instance
            config: Configuration object
        """
        self.search_client = azure_clients.search_client
        self.config = config
        self.api_version = "2023-10-01-Preview"
        self.base_url = f"{config.AZURE_SEARCH_ENDPOINT}/indexes/{config.AZURE_SEARCH_INDEX_NAME}"
        self.headers = {
            "Content-Type": "application/json",
            "api-key": config.AZURE_SEARCH_KEY
        }
    
    def create_index(self):
        """Create or update search index"""
        index_name = self.config.AZURE_SEARCH_INDEX_NAME
        
        try:
            index_definition = self._get_index_definition(index_name)
            url = f"{self.config.AZURE_SEARCH_ENDPOINT}/indexes/{index_name}?api-version={self.api_version}"
            
            response = requests.put(url, headers=self.headers, json=index_definition)
            
            if response.status_code in (200, 201, 204):
                logger.info(f"Successfully created search index: {index_name}")
                return True
            else:
                logger.error(f"Failed to create index: {response.status_code} - {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"Error creating search index '{index_name}': {e}")
            return False
    
    def _get_index_definition(self, index_name):
        """Get index definition with schema"""
        return {
            "name": index_name,
            "fields": [
                {"name": "id", "type": "Edm.String", "key": True, "filterable": True, "searchable": True},
                {"name": "text", "type": "Edm.String", "searchable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_CaseName", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_Citation", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "standard.lucene"},
                {"name": "metadata_CaseNumber", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "standard.lucene"},
                {"name": "metadata_DateOfJudgment", "type": "Edm.String", "searchable": True, "filterable": True, "sortable": True, "facetable": True},
                {"name": "metadata_Bench", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_SubjectMatter", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_Keywords", "type": "Collection(Edm.String)", "searchable": True, "filterable": True, "facetable": True},
                {"name": "metadata_Summary", "type": "Edm.String", "searchable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_PetitionerAdvocates", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_RespondentAdvocates", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_Court", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True, "analyzer": "en.microsoft"},
                {"name": "metadata_OriginalJudgmentURL", "type": "Edm.String", "filterable": True, "searchable": True},
                {"name": "metadata_ChunkId", "type": "Edm.Int32", "filterable": True},
                {"name": "metadata_ChunkTotal", "type": "Edm.Int32", "filterable": True},
                {"name": "metadata_DocumentId", "type": "Edm.String", "filterable": True, "searchable": True},
                {
                    "name": "embedding",
                    "type": "Collection(Edm.Single)",
                    "dimensions": 1536,
                    "vectorSearchProfile": "my-profile"
                }
            ],
            "vectorSearch": {
                "algorithms": [
                    {
                        "name": "my-algorithm",
                        "kind": "hnsw",
                        "hnswParameters": {
                            "m": 10,
                            "efConstruction": 500,
                            "efSearch": 1000,
                            "metric": "cosine"
                        }
                    }
                ],
                "profiles": [
                    {
                        "name": "my-profile",
                        "algorithm": "my-algorithm"
                    }
                ]
            },
            "semantic": {
                "configurations": [
                    {
                        "name": "my-semantic-config",
                        "prioritizedFields": {
                            "titleField": {"fieldName": "metadata_CaseName"},
                            "prioritizedContentFields": [
                                {"fieldName": "text"},
                                {"fieldName": "metadata_Summary"},
                                {"fieldName": "metadata_CaseName"},
                                {"fieldName": "metadata_Citation"},
                                {"fieldName": "metadata_CaseNumber"},
                                {"fieldName": "metadata_DateOfJudgment"},
                                {"fieldName": "metadata_Bench"},
                                {"fieldName": "metadata_SubjectMatter"},
                                {"fieldName": "metadata_PetitionerAdvocates"},
                                {"fieldName": "metadata_RespondentAdvocates"},
                                {"fieldName": "metadata_Court"}
                            ],
                            "prioritizedKeywordsFields": [
                                {"fieldName": "metadata_Keywords"},
                                {"fieldName": "metadata_SubjectMatter"}
                            ]
                        }
                    }
                ]
            }
        }
    
    def upload_chunks(self, chunks, batch_size=None):
        """
        Upload chunks to Azure Cognitive Search
        
        Args:
            chunks: List of chunks with embeddings
            batch_size: Optional batch size override
            
        Returns:
            tuple: (succeeded_count, failed_count)
        """
        if not chunks:
            logger.warning("No chunks to upload")
            return 0, 0
        
        if not batch_size:
            batch_size = self.config.UPLOAD_BATCH_SIZE
        
        search_documents = self._prepare_documents(chunks)
        
        if not search_documents:
            logger.warning("No valid documents to upload")
            return 0, 0
        
        return self._upload_in_batches(search_documents, batch_size)
    
    def _prepare_documents(self, chunks):
        """Prepare chunks for upload"""
        print(f"Preparing {len(chunks)} documents for upload to search index")
        search_documents = []
        
        for chunk in chunks:
            try:
                metadata = chunk["metadata"]
                keywords = metadata.get("Keywords", [])
                if not isinstance(keywords, list):
                    try:
                        keywords = json.loads(keywords) if isinstance(keywords, str) else []
                    except:
                        keywords = []
                
                vector = chunk["vector"]
                if hasattr(vector, 'tolist'):
                    vector = vector.tolist()
                
                from datetime import datetime
                
                # Handle date conversion
                date_str = metadata.get("Date of Judgment", "")
                date_value = None
                if date_str and date_str.strip():
                    try:
                        date_value = datetime.fromisoformat(date_str.replace('Z', '+00:00')).isoformat()
                    except:
                        date_value = None
                
                # Get document ID from chunk metadata
                document_id = chunk.get("blob_name", "")
                if not document_id:
                    document_id = metadata.get("document_id", "")
                
                search_doc = {
                    "@search.action": "upload",
                    "id": chunk["id"],
                    "content": chunk["text"],
                    "content_vector": vector,
                    "pdf_id": document_id,
                    "chunk_index": int(metadata.get("chunk_id", 0)),
                    "chunk_total": int(metadata.get("chunk_total", 0)),
                    "case_name": metadata.get("case_name", "Unknown"),
                    "case_number": metadata.get("case_number", "Unknown"),
                    "citation": metadata.get("citation", "Unknown"),
                    "bench": metadata.get("bench", ""),
                    "court": metadata.get("court", ""),
                    "summary": metadata.get("summary", ""),
                    "keywords": metadata.get("keywords", []),
                    "petitioner_advocates": metadata.get("petitioner_advocates", []),
                    "respondent_advocates": metadata.get("respondent_advocates", [])
                }
                
                # Handle date conversion for search index
                date_str = metadata.get("date_of_judgment", "")
                if date_str and date_str.strip():
                    try:
                        from datetime import datetime
                        # Try to parse and convert to proper datetime format
                        if 'T' in date_str:
                            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        else:
                            # Assume it's a date string, try common formats
                            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                                try:
                                    dt = datetime.strptime(date_str, fmt)
                                    break
                                except:
                                    continue
                            else:
                                dt = None
                        
                        if dt:
                            search_doc["date_of_judgment"] = dt.isoformat() + 'Z'
                    except Exception as e:
                        logger.warning(f"Could not parse date '{date_str}': {e}")
                
                # Add created_at timestamp
                from datetime import datetime
                search_doc["created_at"] = datetime.utcnow().isoformat() + 'Z'
                search_documents.append(search_doc)
            except Exception as e:
                logger.error(f"Error preparing document {chunk.get('id', 'unknown')}: {e}")
        
        return search_documents
    
    def _upload_in_batches(self, search_documents, batch_size):
        """Upload documents in parallel batches"""
        total_docs = len(search_documents)
        num_batches = (total_docs + batch_size - 1) // batch_size
        
        print(f"Uploading {total_docs} documents to search index in {num_batches} batches")
        
        total_succeeded = 0
        total_failed = 0
        completed_batches = 0
        
        with ThreadPoolExecutor(max_workers=min(8, num_batches)) as executor:
            futures = []
            for i in range(0, total_docs, batch_size):
                batch = search_documents[i:i + batch_size]
                batch_num = i // batch_size + 1
                futures.append(executor.submit(self._upload_batch, batch, batch_num))
            
            for future in as_completed(futures):
                try:
                    succeeded, failed = future.result()
                    total_succeeded += succeeded
                    total_failed += failed
                    completed_batches += 1
                    
                    if completed_batches % 5 == 0 or completed_batches == num_batches:
                        print(f"  Progress: {completed_batches}/{num_batches} batches uploaded ({completed_batches * 100 // num_batches}%)")
                except Exception as e:
                    logger.error(f"Error processing batch upload result: {e}")
        
        print(f"Upload complete. Total: {total_docs}, Succeeded: {total_succeeded}, Failed: {total_failed}")
        return total_succeeded, total_failed
    
    def _upload_batch(self, batch_docs, batch_num):
        """Upload a single batch"""
        try:
            batch_payload = {"value": batch_docs}
            url = f"{self.base_url}/docs/index?api-version={self.api_version}&allowUnsafeKeys=true"
            
            for attempt in range(self.config.MAX_RETRIES):
                try:
                    response = requests.post(url, headers=self.headers, json=batch_payload)
                    
                    if response.status_code in (200, 201, 204):
                        result = response.json()
                        succeeded = 0
                        failed = 0
                        
                        if 'value' in result:
                            for item in result.get('value', []):
                                if item.get('status') == False or (item.get('errorMessage') and item.get('errorMessage') != ''):
                                    failed += 1
                                else:
                                    succeeded += 1
                        else:
                            succeeded = len(batch_docs)
                        
                        return succeeded, failed
                    else:
                        if response.status_code in (400, 401, 403):
                            logger.error(f"Batch {batch_num} upload failed: {response.status_code} - {response.text}")
                            return 0, len(batch_docs)
                        
                        if attempt < self.config.MAX_RETRIES - 1:
                            time.sleep(self.config.RETRY_DELAY * (2 ** attempt))
                except requests.exceptions.RequestException as e:
                    if attempt < self.config.MAX_RETRIES - 1:
                        time.sleep(self.config.RETRY_DELAY * (2 ** attempt))
            
            logger.error(f"Failed to upload batch {batch_num} after {self.config.MAX_RETRIES} attempts")
            return 0, len(batch_docs)
        
        except Exception as e:
            logger.error(f"Unexpected error in batch {batch_num} upload: {e}")
            return 0, len(batch_docs)
    
    def is_document_indexed(self, blob_name):
        """Check if a document is already indexed"""
        try:
            import re
            identifier = re.sub(r'[^\w\-]', '_', blob_name)
            
            search_results = self.search_client.search(
                search_text="*",
                filter=f"pdf_id eq '{identifier}'",
                select="id",
                top=1
            )
            
            for result in search_results:
                return True
            
            return False
        except Exception as e:
            logger.warning(f"Error checking if document {blob_name} is indexed: {e}")
            return False
