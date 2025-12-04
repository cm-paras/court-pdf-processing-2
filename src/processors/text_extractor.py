"""
Text extraction from PDF files
"""
import fitz
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils import get_logger

logger = get_logger(__name__)


class TextExtractor:
    """Handles text extraction from PDF files"""
    
    def __init__(self, config):
        """
        Initialize text extractor
        
        Args:
            config: Configuration object
        """
        self.config = config
    
    def extract_text(self, pdf_path):
        """
        Extract text from a PDF file using PyMuPDF
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            str: Extracted text
        """
        try:
            full_text = ""
            with fitz.open(pdf_path) as doc:
                num_pages = len(doc)
                
                for page_num in range(num_pages):
                    try:
                        page = doc[page_num]
                        text = page.get_text(
                            "text",
                            flags=fitz.TEXT_PRESERVE_WHITESPACE | fitz.TEXT_PRESERVE_LIGATURES
                        )
                        full_text += text + "\n\n"
                    except Exception as page_error:
                        logger.error(f"Error extracting text from page {page_num} in {pdf_path}: {page_error}")
            
            if not full_text.strip():
                logger.warning(f"Extracted empty text from {pdf_path}")
                return "No text content could be extracted from this document."
            
            return full_text
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {str(e)}")
            return "Error extracting text from document."
    
    def extract_batch(self, local_paths_dict):
        """
        Extract text from multiple PDFs in parallel
        
        Args:
            local_paths_dict: Dictionary mapping blob names to local paths
            
        Returns:
            dict: Mapping of blob names to extracted text
        """
        results = {}
        
        with ThreadPoolExecutor(
            max_workers=min(len(local_paths_dict), self.config.MAX_EXTRACTION_WORKERS)
        ) as executor:
            future_to_blob = {}
            for blob_name, local_path in local_paths_dict.items():
                future = executor.submit(self.extract_text, local_path)
                future_to_blob[future] = blob_name
            
            for future in as_completed(future_to_blob):
                blob_name = future_to_blob[future]
                try:
                    text = future.result()
                    results[blob_name] = text
                except Exception as e:
                    logger.error(f"Error extracting text from {blob_name}: {e}")
                    results[blob_name] = None
        
        return results
