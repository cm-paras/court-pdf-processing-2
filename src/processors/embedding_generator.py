"""
Embedding generation for document chunks
"""
from src.utils import get_logger

logger = get_logger(__name__)


class EmbeddingGenerator:
    """Handles embedding generation for text chunks"""
    
    def __init__(self, azure_clients, config):
        """
        Initialize embedding generator
        
        Args:
            azure_clients: AzureClientManager instance
            config: Configuration object
        """
        self.openai_client = azure_clients.openai_client
        self.config = config
    
    def generate_embeddings(self, chunks, batch_size=None):
        """
        Generate embeddings for text chunks with adaptive batch sizing
        
        Args:
            chunks: List of chunks with text
            batch_size: Optional batch size override
            
        Returns:
            list: Chunks with embeddings added
        """
        if not batch_size:
            batch_size = self.config.EMBEDDING_BATCH_SIZE
        
        total_chunks = len(chunks)
        num_batches = (total_chunks + batch_size - 1) // batch_size
        
        print(f"Generating embeddings for {total_chunks} chunks in {num_batches} batches")
        
        for i in range(0, total_chunks, batch_size):
            batch = chunks[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"  Processing embedding batch {batch_num}/{num_batches} ({len(batch)} chunks)")
            
            batch_texts = [chunk["text"] for chunk in batch]
            
            try:
                response = self.openai_client.embeddings.create(
                    model="text-embedding-3-small",
                    input=batch_texts
                )
                
                for j, embedding_data in enumerate(response.data):
                    if j < len(batch):
                        batch[j]["vector"] = embedding_data.embedding
            
            except Exception as e:
                logger.error(f"Error generating embeddings for batch: {e}")
                logger.info("Falling back to individual embedding generation")
                self._generate_individually(batch)
        
        print(f"Completed embedding generation for {total_chunks} chunks")
        return chunks
    
    def _generate_individually(self, chunks):
        """Fallback method to generate embeddings one by one"""
        for chunk in chunks:
            try:
                response = self.openai_client.embeddings.create(
                    model="text-embedding-3-small",
                    input=chunk["text"]
                )
                chunk["vector"] = response.data[0].embedding
            except Exception as e:
                logger.error(f"Error generating individual embedding: {e}")
                # Create a vector of zeros as fallback
                chunk["vector"] = [0.0] * 1536
