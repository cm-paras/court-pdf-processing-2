"""
Azure client initialization and management
"""
import requests
from urllib3.util.retry import Retry
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from openai import AzureOpenAI

import logging

logger = logging.getLogger(__name__)


class AzureClientManager:
    """Manages Azure service clients"""
    
    def __init__(self, config):
        """
        Initialize Azure clients
        
        Args:
            config: Configuration object with Azure credentials
        """
        self.config = config
        
        # Initialize clients
        self.blob_service_client = self._init_blob_client()
        self.container_client = self.blob_service_client.get_container_client(
            config.BLOB_CONTAINER_NAME
        )
        
        self.cosmos_client = self._init_cosmos_client()
        self.database = self.cosmos_client.get_database_client(config.COSMOS_DB_DATABASE)
        self.cosmos_container = self.database.get_container_client(config.COSMOS_DB_CONTAINER)
        
        self.openai_client = self._init_openai_client()
        
        self.search_credential = AzureKeyCredential(config.AZURE_SEARCH_KEY)
        self.search_index_client = self._init_search_index_client()
        self.search_client = self._init_search_client()
        
        self.session = self._init_http_session()
        
        logger.info("Azure clients initialized successfully")
        logger.info(f"Using Azure OpenAI chat model: {config.AZURE_OPENAI_CHAT_MODEL}")
    
    def _init_blob_client(self):
        """Initialize Azure Blob Storage client"""
        return BlobServiceClient.from_connection_string(
            self.config.AZURE_STORAGE_CONNECTION_STRING
        )
    
    def _init_cosmos_client(self):
        """Initialize Azure Cosmos DB client"""
        return CosmosClient(
            url=self.config.COSMOS_DB_ENDPOINT,
            credential=self.config.COSMOS_DB_KEY
        )
    
    def _init_openai_client(self):
        """Initialize Azure OpenAI client"""
        return AzureOpenAI(
            api_key=self.config.AZURE_OPENAI_API_KEY,
            api_version="2024-05-01-preview",
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT
        )
    
    def _init_search_index_client(self):
        """Initialize Azure Cognitive Search Index client"""
        return SearchIndexClient(
            endpoint=self.config.AZURE_SEARCH_ENDPOINT,
            credential=self.search_credential
        )
    
    def _init_search_client(self):
        """Initialize Azure Cognitive Search client"""
        return SearchClient(
            endpoint=self.config.AZURE_SEARCH_ENDPOINT,
            index_name=self.config.AZURE_SEARCH_INDEX_NAME,
            credential=self.search_credential
        )
    
    def _init_http_session(self):
        """Initialize HTTP session with retry strategy"""
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        
        adapter = requests.adapters.HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=100,
            pool_maxsize=100
        )
        
        session = requests.Session()
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        return session
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
        except Exception as e:
            logger.warning(f"Error during client cleanup: {e}")
