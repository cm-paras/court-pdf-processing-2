"""
Configuration management for PDF processing pipeline
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration class for Azure services and processing parameters"""
    
    # Azure Blob Storage
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")

    # Azure Cosmos DB
    COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
    COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
    COSMOS_DB_DATABASE = os.getenv("COSMOS_DB_DATABASE")
    COSMOS_DB_CONTAINER = os.getenv("COSMOS_DB_CONTAINER")

    # Azure OpenAI
    AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
    AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
    AZURE_OPENAI_CHAT_MODEL = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4.1-mini")

    # Azure Cognitive Search
    AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
    AZURE_SEARCH_KEY = os.getenv("AZURE_SEARCH_KEY")
    AZURE_SEARCH_INDEX_NAME = os.getenv("AZURE_SEARCH_INDEX_NAME")
    
    # Processing configuration
    MAX_BATCH_SIZE = 100
    MAX_WORKERS = 8
    MAX_EXTRACTION_WORKERS = 8
    MAX_EMBEDDING_WORKERS = 4
    MAX_RETRIES = 5
    RETRY_DELAY = 5
    CHUNK_SIZE = 2500
    CHUNK_OVERLAP = 200
    EMBEDDING_BATCH_SIZE = 10
    UPLOAD_BATCH_SIZE = 50
    COSMOS_BATCH_SIZE = 50
    TEXT_EXTRACTION_BATCH_SIZE = 20
    
    # Memory management
    CHECKPOINTING_INTERVAL = 500
    
    # Caching configuration
    METADATA_CACHE_SIZE = 10000
    EMBEDDING_CACHE_SIZE = 20000
    
    @classmethod
    def validate(cls):
        """Validate that all required environment variables are set"""
        required_vars = [
            'AZURE_STORAGE_CONNECTION_STRING', 'BLOB_CONTAINER_NAME',
            'COSMOS_DB_ENDPOINT', 'COSMOS_DB_KEY',
            'COSMOS_DB_DATABASE', 'COSMOS_DB_CONTAINER',
            'AZURE_OPENAI_API_KEY', 'AZURE_OPENAI_ENDPOINT',
            'AZURE_SEARCH_ENDPOINT', 'AZURE_SEARCH_KEY', 'AZURE_SEARCH_INDEX_NAME'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True
