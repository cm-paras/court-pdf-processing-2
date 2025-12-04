"""
PDF download functionality
"""
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils import get_logger

logger = get_logger(__name__)


class PDFDownloader:
    """Handles PDF downloading from Azure Blob Storage or URLs"""
    
    def __init__(self, azure_clients, config):
        """
        Initialize PDF downloader
        
        Args:
            azure_clients: AzureClientManager instance
            config: Configuration object
        """
        self.container_client = azure_clients.container_client
        self.session = azure_clients.session
        self.config = config
    
    def download_pdf(self, pdf_url, local_path=None):
        """
        Download a PDF blob to a local file
        
        Args:
            pdf_url: URL or blob name of the PDF
            local_path: Optional local path to save the PDF
            
        Returns:
            str: Path to the downloaded PDF
        """
        if not local_path:
            temp_file = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
            local_path = temp_file.name
            temp_file.close()
        
        try:
            if pdf_url.startswith('http'):
                self._download_from_url(pdf_url, local_path)
            else:
                self._download_from_blob(pdf_url, local_path)
            
            logger.debug(f"Successfully downloaded PDF from {pdf_url} to {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Error downloading {pdf_url}: {e}")
            self._cleanup_file(local_path)
            raise
    
    def _download_from_url(self, url, local_path):
        """Download PDF from direct URL"""
        response = self.session.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(local_path, "wb") as pdf_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    pdf_file.write(chunk)
    
    def _download_from_blob(self, blob_name, local_path):
        """Download PDF from Azure Blob Storage"""
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(local_path, "wb") as pdf_file:
            download_stream = blob_client.download_blob()
            pdf_file.write(download_stream.readall())
    
    def download_batch(self, blob_names):
        """
        Download multiple PDFs in parallel with rate limiting
        
        Args:
            blob_names: List of blob names or URLs
            
        Returns:
            dict: Mapping of blob names to local paths
        """
        local_paths = {}
        
        with ThreadPoolExecutor(max_workers=min(len(blob_names), 4)) as executor:
            future_to_blob = {
                executor.submit(self.download_pdf, blob_name): blob_name 
                for blob_name in blob_names
            }
            
            for future in as_completed(future_to_blob):
                blob_name = future_to_blob[future]
                try:
                    local_path = future.result()
                    local_paths[blob_name] = local_path
                except Exception as e:
                    logger.error(f"Error downloading {blob_name}: {e}")
        
        return local_paths
    
    @staticmethod
    def _cleanup_file(file_path):
        """Clean up a file"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.warning(f"Error cleaning up file {file_path}: {e}")
