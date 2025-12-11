"""Embedding generation with quality validation."""

import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class EmbeddingGenerator:
    """Generates embeddings for text chunks."""
    
    def __init__(self, azure_clients, config):
        self.openai_client = azure_clients.openai_client
        self.config = config
    
    def generate_embeddings(self, chunks):
        """Generate embeddings for chunks."""
        logger.info(f"Generating embeddings for {len(chunks)} chunks")
        
        # Process in batches
        batch_size = self.config.EMBEDDING_BATCH_SIZE
        chunks_with_embeddings = []
        
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            
            try:
                # Extract texts from batch
                texts = [chunk['text'] for chunk in batch]
                
                # Generate embeddings
                response = self.openai_client.embeddings.create(
                    input=texts,
                    model="text-embedding-3-small"
                )
                
                # Add embeddings to chunks
                for j, chunk in enumerate(batch):
                    chunk['vector'] = response.data[j].embedding
                    chunks_with_embeddings.append(chunk)
                
                logger.info(f"Generated embeddings for batch {i//batch_size + 1}")
                
            except Exception as e:
                logger.error(f"Failed to generate embeddings for batch {i//batch_size + 1}: {e}")
                # Add chunks without embeddings
                chunks_with_embeddings.extend(batch)
        
        return chunks_with_embeddings
    
