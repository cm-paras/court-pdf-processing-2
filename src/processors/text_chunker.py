"""Text chunking processor."""

import logging
import uuid
from typing import List
from datetime import datetime
from langchain_text_splitters import RecursiveCharacterTextSplitter

from ..config.settings import CONFIG
from ..models.document import Document, Chunk, ChunkStatus
from ..utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

class TextChunker:
    """Chunks document text into smaller pieces."""
    
    def __init__(self):
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=CONFIG.CHUNK_SIZE,
            chunk_overlap=CONFIG.CHUNK_OVERLAP,
            separators=CONFIG.CHUNK_SEPARATORS,
            length_function=len
        )
    
    def chunk_document(self, document: Document) -> List[Chunk]:
        """Chunk document text into smaller pieces."""
        document.processing_timestamps.chunking_start = datetime.utcnow()
        
        try:
            logger.info(f"Chunking document {document.pdf_id}")
            
            # Split text into chunks
            text_chunks = self.splitter.split_text(document.full_text)
            
            # Filter out short chunks
            valid_chunks = [chunk for chunk in text_chunks if len(chunk.strip()) >= CONFIG.CHUNK_MIN_LENGTH]
            
            if not valid_chunks:
                logger.warning(f"No valid chunks created for document {document.pdf_id}")
                return []
            
            # Create chunk objects
            chunks = []
            for i, chunk_text in enumerate(valid_chunks):
                chunk = Chunk(
                    chunk_id=f"{document.pdf_id}_chunk_{i}",
                    pdf_id=document.pdf_id,
                    text=chunk_text.strip(),
                    metadata=document.metadata_json.copy(),
                    chunk_index=i,
                    chunk_total=len(valid_chunks),
                    status=ChunkStatus.PENDING_EMBEDDING
                )
                chunks.append(chunk)
            
            document.processing_timestamps.chunking_end = datetime.utcnow()
            
            logger.info(f"Created {len(chunks)} chunks for document {document.pdf_id}")
            return chunks
            
        except Exception as e:
            logger.error(f"Chunking failed for document {document.pdf_id}: {e}")
            document.processing_timestamps.chunking_end = datetime.utcnow()
            raise