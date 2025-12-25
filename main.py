"""Main entry point for the PDF processing pipeline."""

import argparse
import pickle
import logging
import os
from typing import List, Tuple

from src.utils.logging_config import setup_logging
from src.pipeline.pdf_processor import PDFProcessor
from src.clients.azure_clients import AzureClientManager
from src.config.config import Config

def load_pdf_urls(file_path: str = "url.pkl") -> List[Tuple[str, str]]:
    """Load PDF URLs from pickle file."""
    try:
        with open(file_path, 'rb') as f:
            urls = pickle.load(f)
        
        # Convert to (blob_url, pdf_id) tuples
        pdf_list = []
        for i, url in enumerate(urls):
            pdf_id = f"pdf_{i:06d}"  # Generate consistent IDs
            pdf_list.append((url, pdf_id))
        
        logging.info(f"Loaded {len(pdf_list)} PDF URLs from {file_path}")
        return pdf_list
        
    except Exception as e:
        logging.error(f"Failed to load PDF URLs: {e}")
        return []

def clear_search_index():
    """Recreate search index (faster than clearing)."""
    import subprocess
    import sys
    
    try:
        logging.info("Recreating search index...")
        
        # Run the recreate script
        script_path = os.path.join(os.path.dirname(__file__), "scripts", "recreate_index.py")
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, check=True)
        
        logging.info("Search index recreated successfully")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to recreate index: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"Failed to recreate index: {e}")
        raise

def main():
    """Main processing function."""
    parser = argparse.ArgumentParser(description="PDF Processing Pipeline")
    parser.add_argument("--max_pdfs", type=int, help="Maximum number of PDFs to process")
    parser.add_argument("--log_level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--url_file", default="url.pkl", help="Path to URL pickle file")
    parser.add_argument("--clear_index", action="store_true", help="Clear search index before processing")
    parser.add_argument("--index_only", action="store_true", help="Only index documents from Cosmos DB (skip PDF processing)")
    parser.add_argument("--metadata_only", action="store_true", help="Only extract metadata from PDFs (skip indexing)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    logging.info("Starting PDF Processing Pipeline")
    logging.info(f"Configuration: max_pdfs={args.max_pdfs}, log_level={args.log_level}")
    
    try:
        # Clear index if requested
        if args.clear_index:
            clear_search_index()
        
        # Determine processing mode
        if args.index_only:
            logging.info("Running in INDEX-ONLY mode")
            from src.pipeline.pipeline import PDFProcessingPipeline
            from src.config.config import Config

            config = Config()
            pipeline = PDFProcessingPipeline(config)
            indexing_results = pipeline.index_from_cosmos()

            logging.info("=" * 50)
            logging.info("INDEXING SUMMARY")
            logging.info("=" * 50)
            logging.info(f"Documents in Cosmos: {indexing_results['total_documents']}")
            logging.info(f"Chunks indexed: {indexing_results['indexed_chunks']}")
            return
        
        # Load PDF URLs (only needed for metadata_only and full processing)
        pdf_urls = load_pdf_urls(args.url_file)
        if not pdf_urls:
            logging.error("No PDF URLs loaded, exiting")
            return
        
        # Initialize processor
        processor = PDFProcessor()
        
        if args.metadata_only:
            logging.info("Running in METADATA-ONLY mode")
            results = processor.process_batch(pdf_urls, args.max_pdfs, mode="metadata")
            logging.info("=" * 50)
            logging.info("METADATA PROCESSING SUMMARY")
            logging.info("=" * 50)
            logging.info(f"Total PDFs: {results['total']}")
            logging.info(f"Successful: {results['successful']}")
            logging.info(f"Failed: {results['failed']}")
            logging.info(f"Success Rate: {results['successful']/results['total']*100:.1f}%")
            return

        else:
            logging.info("Running in FULL PROCESSING mode (metadata + indexing)")
            results = processor.process_batch(pdf_urls, args.max_pdfs, mode="full")

            logging.info("=" * 50)
            logging.info("FULL PROCESSING SUMMARY")
            logging.info("=" * 50)
            logging.info(f"Total PDFs: {results['total']}")
            logging.info(f"Successful: {results['successful']}")
            logging.info(f"Failed: {results['failed']}")
            logging.info(f"Success Rate: {results['successful']/results['total']*100:.1f}%")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
