"""Configuration settings for the PDF processing pipeline."""

import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class ProcessingConfig:
    """Processing configuration parameters."""
    
    # OCR Settings
    OCR_MIN_TEXT_LENGTH: int = 1000
    OCR_MIN_CONFIDENCE: float = 0.40
    
    # Chunking Settings
    CHUNK_SIZE: int = 800
    CHUNK_OVERLAP: int = 80
    CHUNK_MIN_LENGTH: int = 150
    CHUNK_SEPARATORS: List[str] = None
    
    # Embedding Settings
    EMBEDDING_MODEL: str = "text-embedding-3-small"
    EMBEDDING_DIMENSION: int = 3072
    EMBEDDING_MIN_NORM: float = 1e-6
    EMBEDDING_BATCH_SIZE: int = 10
    
    # Retry Settings
    MAX_RETRIES: int = 5
    BASE_DELAY: float = 2.0
    
    # File Validation
    MIN_PDF_SIZE: int = 1024  # 1KB
    
    def __post_init__(self):
        if self.CHUNK_SEPARATORS is None:
            self.CHUNK_SEPARATORS = ["\n\n", "\n", ". ", " ", ""]

@dataclass
class AzureConfig:
    """Azure service configuration."""
    
    # Blob Storage
    STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    BLOB_CONTAINER_NAME: str = os.getenv("BLOB_CONTAINER_NAME")
    
    # Cosmos DB
    COSMOS_ENDPOINT: str = os.getenv("COSMOS_DB_ENDPOINT")
    COSMOS_KEY: str = os.getenv("COSMOS_DB_KEY")
    COSMOS_DATABASE: str = os.getenv("COSMOS_DB_DATABASE")
    COSMOS_CONTAINER: str = os.getenv("COSMOS_DB_CONTAINER")
    
    # OpenAI
    OPENAI_API_KEY: str = os.getenv("AZURE_OPENAI_API_KEY")
    OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT")
    
    # Gemini (for metadata)
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    
    # Search
    SEARCH_ENDPOINT: str = os.getenv("AZURE_SEARCH_ENDPOINT")
    SEARCH_KEY: str = os.getenv("AZURE_SEARCH_KEY")
    SEARCH_INDEX_NAME: str = os.getenv("AZURE_SEARCH_INDEX_NAME")

# Global configuration instances
CONFIG = ProcessingConfig()
AZURE_CONFIG = AzureConfig()