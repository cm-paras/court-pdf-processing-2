"""Main PDF processing pipeline."""

import logging
from typing import List, Optional

from ..config.config import Config
from ..clients.azure_clients import AzureClientManager
from ..processors.pdf_downloader import PDFDownloader
from ..processors.text_extractor import TextExtractor
from ..processors.metadata_extractor import MetadataExtractor
from ..processors.document_chunker import DocumentChunker
from ..processors.embedding_generator import EmbeddingGenerator
from ..storage.cosmos_storage import CosmosStorage
from ..storage.search_indexer import SearchIndexer

logger = logging.getLogger(__name__)

class PDFProcessor:
    """Sequential PDF processing pipeline."""
    
    def __init__(self):
        self.config = Config()
        self.config.validate()
        
        self.azure_clients = AzureClientManager(self.config)
        
        self.downloader = PDFDownloader(self.azure_clients, self.config)
        self.text_extractor = TextExtractor(self.config)
        self.metadata_extractor = MetadataExtractor(self.azure_clients, self.config)
        self.chunker = DocumentChunker(self.config)
        self.embedding_generator = EmbeddingGenerator(self.azure_clients, self.config)
        self.storage = CosmosStorage(self.azure_clients, self.config)
        self.indexer = SearchIndexer(self.azure_clients, self.config)
    
    def process_single_pdf(self, blob_url: str, pdf_id: str) -> bool:
        """Process a single PDF through the complete pipeline."""
        logger.info(f"Starting processing for PDF {pdf_id}")
        
        try:
            # Download PDF
            local_paths = self.downloader.download_batch([blob_url])
            if not local_paths:
                logger.error(f"Failed to download PDF {pdf_id}")
                return False
            
            # Extract text
            texts = self.text_extractor.extract_batch(local_paths)
            if not texts:
                logger.error(f"Failed to extract text from PDF {pdf_id}")
                return False
            
            # Extract metadata
            metadata = self.metadata_extractor.extract_batch(texts)
            if not metadata:
                logger.error(f"Failed to extract metadata from PDF {pdf_id}")
                return False
            
            # Store in Cosmos DB
            blob_name = list(metadata.keys())[0]
            text_sample = list(texts.values())[0][:1000]
            
            success = self.storage.store_document(
                blob_name,
                list(metadata.values())[0],
                text_sample
            )
            
            if not success:
                logger.error(f"Failed to store document {pdf_id} in Cosmos DB")
                return False
            
            # Chunk and index
            documents = [{
                "blob_name": blob_name,
                "success": True,
                "metadata": list(metadata.values())[0],
                "text": list(texts.values())[0]
            }]
            
            chunks = self.chunker.chunk_batch(documents)
            if chunks:
                chunks_with_embeddings = self.embedding_generator.generate_embeddings(chunks)
                succeeded, failed = self.indexer.upload_chunks(chunks_with_embeddings)
                
                if succeeded > 0:
                    logger.info(f"Successfully processed PDF {pdf_id}: {succeeded} chunks indexed")
                    return True
            
            logger.error(f"Failed to index chunks for PDF {pdf_id}")
            return False
            
        except Exception as e:
            logger.error(f"Processing failed for PDF {pdf_id}: {e}")
            return False
    

    
    def process_batch(self, pdf_urls: List[tuple], max_pdfs: Optional[int] = None) -> dict:
        """Process multiple PDFs sequentially."""
        results = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
        pdfs_to_process = pdf_urls[:max_pdfs] if max_pdfs else pdf_urls
        
        for blob_url, pdf_id in pdfs_to_process:
            results['total'] += 1
            
            # Check if already processed
            if self.storage.document_exists(pdf_id) and self.indexer.is_document_indexed(pdf_id):
                logger.info(f"Skipping already processed PDF {pdf_id}")
                results['skipped'] += 1
                continue
            
            try:
                success = self.process_single_pdf(blob_url, pdf_id)
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                    
            except Exception as e:
                logger.error(f"Unexpected error processing {pdf_id}: {e}")
                results['failed'] += 1
        
        logger.info(f"Batch processing complete: {results}")
        return results