"""
PDF Processing and Metadata Extraction Pipeline - Main Entry Point
Modular architecture with proper package structure
"""
import argparse
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.config import Config
from src.utils import configure_logging
from src.pipeline import PDFProcessingPipeline
from src.utils import unpickle_list, divide_list

# Configure logging
logger = configure_logging()


def main():
    """Main entry point for the PDF processing pipeline"""
    parser = argparse.ArgumentParser(description="PDF Processing and Indexing Pipeline")
    parser.add_argument("--max_pdfs", type=int, help="Maximum number of PDFs to process", default=None)
    parser.add_argument("--checkpoint", type=str, help="Path to checkpoint file", default=None)
    parser.add_argument("--skip_to_indexing", action="store_true", help="Skip PDF processing and go straight to indexing")
    parser.add_argument("--incremental", action="store_true", help="Run incremental update")
    parser.add_argument("-c", type=int, help="Total server count", default=1)
    parser.add_argument("-s", type=int, help="Current server number (0-indexed)", default=0)
    args = parser.parse_args()
    
    pipeline = None
    
    try:
        # Initialize pipeline
        logger.info("Initializing PDF processing pipeline")
        pipeline = PDFProcessingPipeline(Config, args.c, args.s)
        
        # Load PDF list
        all_pdfs = unpickle_list("url.pkl")
        pdf_blobs = divide_list(all_pdfs, args.c, args.s)
        
        if args.max_pdfs:
            pdf_blobs = pdf_blobs[:args.max_pdfs]
        
        logger.info(f"Processing {len(pdf_blobs)} PDF blobs")
        
        # Run pipeline
        results = pipeline.run(
            pdf_blobs=pdf_blobs,
            max_pdfs=args.max_pdfs,
            skip_to_indexing=args.skip_to_indexing
        )
        
        logger.info(f"Pipeline execution completed: {results}")
        print(f"\nPipeline Results: {results}")
        
        return 0
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
        return 1
    
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        if pipeline:
            try:
                pipeline.cleanup()
                logger.info("Pipeline cleanup completed")
            except Exception as e:
                logger.warning(f"Error during final cleanup: {e}")


if __name__ == "__main__":
    sys.exit(main())
