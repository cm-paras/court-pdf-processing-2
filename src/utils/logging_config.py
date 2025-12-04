"""
Logging configuration for the PDF processing pipeline
"""
import logging
import pickle


def configure_logging():
    """Configure logging to show progress but hide detailed HTTP requests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("pdf_processing.log"),
            logging.StreamHandler()
        ]
    )
    
    # Suppress verbose Azure SDK logging
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("azure.cosmos").setLevel(logging.WARNING)
    logging.getLogger("azure.storage").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("azure.search").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("azure.openai").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)


def get_logger(name):
    """Get a logger instance for a module"""
    return logging.getLogger(name)


def unpickle_list(file_path):
    """Load a list from a pickle file"""
    try:
        with open(file_path, 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        print(f"Error loading pickle file {file_path}: {e}")
        return []


def divide_list(items, total_servers, server_id):
    """Divide a list among multiple servers"""
    if not items or total_servers <= 0 or server_id < 0 or server_id >= total_servers:
        return []
    
    chunk_size = len(items) // total_servers
    remainder = len(items) % total_servers
    
    start_idx = server_id * chunk_size + min(server_id, remainder)
    end_idx = start_idx + chunk_size + (1 if server_id < remainder else 0)
    
    return items[start_idx:end_idx]
