"""Cosmos DB client for document and chunk storage."""

import logging
from typing import List, Optional, Dict, Any
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from ..config.settings import AZURE_CONFIG
from ..models.document import Document, Chunk, DocumentStatus, ChunkStatus

logger = logging.getLogger(__name__)

class CosmosStorage:
    """Cosmos DB storage client."""
    
    def __init__(self):
        self.client = CosmosClient(AZURE_CONFIG.COSMOS_ENDPOINT, AZURE_CONFIG.COSMOS_KEY)
        self.database = self.client.get_database_client(AZURE_CONFIG.COSMOS_DATABASE)
        self.container = self.database.get_container_client(AZURE_CONFIG.COSMOS_CONTAINER)
    
    def upsert_document(self, document: Document) -> None:
        """Upsert document record."""
        try:
            doc_dict = document.to_dict()
            doc_dict['id'] = document.pdf_id
            doc_dict['type'] = 'document'
            self.container.upsert_item(doc_dict)
            logger.info(f"Upserted document {document.pdf_id}")
        except Exception as e:
            logger.error(f"Failed to upsert document {document.pdf_id}: {e}")
            raise
    
    def get_document(self, pdf_id: str) -> Optional[Document]:
        """Get document by ID."""
        try:
            item = self.container.read_item(item=pdf_id, partition_key=pdf_id)
            if item.get('type') == 'document':
                return Document.from_dict(item)
            return None
        except CosmosResourceNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to get document {pdf_id}: {e}")
            raise
    
    def upsert_chunk(self, chunk: Chunk) -> None:
        """Upsert chunk record."""
        try:
            chunk_dict = chunk.to_dict()
            chunk_dict['id'] = chunk.chunk_id
            chunk_dict['type'] = 'chunk'
            self.container.upsert_item(chunk_dict)
            logger.debug(f"Upserted chunk {chunk.chunk_id}")
        except Exception as e:
            logger.error(f"Failed to upsert chunk {chunk.chunk_id}: {e}")
            raise
    
    def get_chunks_by_pdf_id(self, pdf_id: str) -> List[Chunk]:
        """Get all chunks for a PDF."""
        try:
            query = "SELECT * FROM c WHERE c.pdf_id = @pdf_id AND c.type = 'chunk'"
            items = list(self.container.query_items(
                query=query,
                parameters=[{"name": "@pdf_id", "value": pdf_id}],
                enable_cross_partition_query=True
            ))
            return [Chunk.from_dict(item) for item in items]
        except Exception as e:
            logger.error(f"Failed to get chunks for PDF {pdf_id}: {e}")
            raise
    
    def get_pending_documents(self, limit: Optional[int] = None) -> List[Document]:
        """Get documents that need processing."""
        try:
            query = """
            SELECT * FROM c 
            WHERE c.type = 'document' 
            AND c.status IN ('pending', 'download_failed', 'ocr_failed', 'metadata_failed')
            """
            items = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            documents = [Document.from_dict(item) for item in items]
            return documents[:limit] if limit else documents
        except Exception as e:
            logger.error(f"Failed to get pending documents: {e}")
            raise
    
    def get_chunks_for_indexing(self, pdf_id: str) -> List[Chunk]:
        """Get chunks ready for indexing."""
        try:
            query = """
            SELECT * FROM c 
            WHERE c.pdf_id = @pdf_id 
            AND c.type = 'chunk' 
            AND c.status = 'embedded'
            """
            items = list(self.container.query_items(
                query=query,
                parameters=[{"name": "@pdf_id", "value": pdf_id}],
                enable_cross_partition_query=True
            ))
            return [Chunk.from_dict(item) for item in items]
        except Exception as e:
            logger.error(f"Failed to get chunks for indexing {pdf_id}: {e}")
            raise
    
    def mark_chunks_indexed(self, chunk_ids: List[str]) -> None:
        """Mark chunks as indexed."""
        try:
            for chunk_id in chunk_ids:
                # Read current chunk
                chunk_item = self.container.read_item(item=chunk_id, partition_key=chunk_id)
                chunk_item['status'] = ChunkStatus.INDEXED.value
                self.container.upsert_item(chunk_item)
            logger.info(f"Marked {len(chunk_ids)} chunks as indexed")
        except Exception as e:
            logger.error(f"Failed to mark chunks as indexed: {e}")
            raise