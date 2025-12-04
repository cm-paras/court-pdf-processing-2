# PDF Processing Pipeline - Modular Architecture

A professional, modular Python application for processing legal PDFs, extracting metadata, and indexing them in Azure Cognitive Search.

## 📁 Project Structure

```
search-index-cloner/
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── config.py              # Configuration management
│   ├── clients/
│   │   ├── __init__.py
│   │   └── azure_clients.py       # Azure service clients
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── pdf_downloader.py      # PDF download functionality
│   │   ├── text_extractor.py      # Text extraction from PDFs
│   │   ├── metadata_extractor.py  # AI-powered metadata extraction
│   │   ├── document_chunker.py    # Document chunking
│   │   └── embedding_generator.py # Vector embedding generation
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── cosmos_storage.py      # Cosmos DB operations
│   │   └── search_indexer.py      # Azure Search indexing
│   ├── pipeline/
│   │   ├── __init__.py
│   │   └── pipeline.py            # Pipeline orchestration
│   └── utils/
│       ├── __init__.py
│       └── logging_config.py      # Logging configuration
├── main.py                         # Main entry point
├── utils.py                        # Helper utilities
├── .env                            # Environment variables
├── url.pkl                         # PDF URLs
└── README.md                       # This file
```

## 🌟 Features

- **Modular Architecture**: Clean separation of concerns with OOP design
- **Azure Integration**: Full integration with Azure Blob Storage, Cosmos DB, OpenAI, and Cognitive Search
- **Parallel Processing**: Multi-threaded processing at each stage
- **Batch Operations**: Efficient batch processing for large datasets
- **Error Handling**: Comprehensive error handling with retries
- **Progress Tracking**: Real-time progress indicators
- **Scalable**: Supports distributed processing across multiple servers

## 🚀 Quick Start

### Prerequisites

```bash
# Python 3.8+
python --version

# Install dependencies
pip install -r requirements.txt
```

### Environment Setup

Create a `.env` file with the following variables:

```env
# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
BLOB_CONTAINER_NAME=your_container

# Azure Cosmos DB
COSMOS_DB_ENDPOINT=your_endpoint
COSMOS_DB_KEY=your_key
COSMOS_DB_DATABASE=your_database
COSMOS_DB_CONTAINER=your_container

# Azure OpenAI
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=your_endpoint
AZURE_OPENAI_CHAT_MODEL=gpt-4-mini

# Azure Cognitive Search
AZURE_SEARCH_ENDPOINT=your_search_endpoint
AZURE_SEARCH_KEY=your_search_key
AZURE_SEARCH_INDEX_NAME=your_index_name
```

### Basic Usage

```bash
# Process all PDFs
python main.py

# Process limited number of PDFs
python main.py --max_pdfs 100

# Skip to indexing (if PDFs already processed)
python main.py --skip_to_indexing

# Distributed processing (server 1 of 3)
python main.py -c 3 -s 0
```

## 📚 Module Documentation

### Configuration (`src/config/`)
- **Config**: Centralized configuration with validation
- Environment variable management
- Processing parameters (batch sizes, workers, retries)

### Clients (`src/clients/`)
- **AzureClientManager**: Manages all Azure service clients
- HTTP session with retry logic
- Resource cleanup

### Processors (`src/processors/`)
- **PDFDownloader**: Downloads PDFs from Azure Blob or URLs
- **TextExtractor**: Extracts text using PyMuPDF
- **MetadataExtractor**: Extracts metadata using Azure OpenAI
- **DocumentChunker**: Splits documents into chunks
- **EmbeddingGenerator**: Generates vector embeddings

### Storage (`src/storage/`)
- **CosmosStorage**: Handles Cosmos DB operations
- **SearchIndexer**: Manages Azure Cognitive Search

### Pipeline (`src/pipeline/`)
- **PDFProcessingPipeline**: Orchestrates the entire workflow
- Batch processing with progress tracking
- Error handling and recovery

### Utils (`src/utils/`)
- **Logging**: Centralized logging configuration
- Suppresses verbose Azure SDK logs

## 🔧 Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--max_pdfs` | Maximum number of PDFs to process | None (all) |
| `--skip_to_indexing` | Skip PDF processing, only index | False |
| `-c` | Total number of servers | 1 |
| `-s` | Current server number (0-indexed) | 0 |

## 🏗️ Architecture Benefits

### 1. Modularity
- Each module has a single, well-defined responsibility
- Easy to understand, test, and maintain
- Components can be reused independently

### 2. Object-Oriented Design
- Classes encapsulate related functionality
- Clear interfaces between components
- Better code organization

### 3. Separation of Concerns
- Configuration separated from logic
- Logging centralized
- Azure clients managed separately

### 4. Maintainability
- Smaller files (50-200 lines each vs 2000+ lines)
- Clear naming conventions
- Comprehensive docstrings

### 5. Testability
- Each module can be unit tested
- Easy to mock dependencies
- Clear input/output contracts

## 📊 Performance

- **Parallel Processing**: Multi-threaded at each stage
- **Batch Operations**: Efficient batch processing
- **Connection Pooling**: Reuses HTTP connections
- **Retry Mechanisms**: Automatic retry with exponential backoff

## 🔄 Pipeline Workflow

```
1. PDF Download
   ↓
2. Text Extraction
   ↓
3. Metadata Extraction (AI)
   ↓
4. Cosmos DB Storage
   ↓
5. Document Chunking
   ↓
6. Embedding Generation
   ↓
7. Search Index Upload
```

## 🛠️ Development

### Adding New Features

1. Create a new module in the appropriate directory
2. Add a class with clear docstrings
3. Integrate with `pipeline.py`
4. Update this README

### Testing

```bash
# Test with a small batch
python main.py --max_pdfs 5
```

## 📝 Dependencies

- `azure-storage-blob` - Azure Blob Storage
- `azure-cosmos` - Azure Cosmos DB
- `azure-search-documents` - Azure Cognitive Search
- `openai` - Azure OpenAI
- `pymupdf` (fitz) - PDF text extraction
- `langchain-text-splitters` - Document chunking
- `requests` - HTTP client
- `tqdm` - Progress bars
- `python-dotenv` - Environment variables

## 🤝 Contributing

1. Follow the existing code structure
2. Add docstrings to all classes and methods
3. Keep modules focused and small
4. Update README for new features

## 📄 License

Same as the original project.

## 🆚 Migration from Original

The original monolithic `main.py` (2000+ lines) has been refactored into this modular structure:

- **Before**: One large file
- **After**: 12 focused modules in organized directories
- **Compatibility**: Same functionality, same CLI arguments
- **Benefits**: Better maintainability, testability, and reusability

## 🐛 Troubleshooting

### Import Errors
Ensure you're running from the project root directory.

### Azure Connection Issues
Verify your `.env` file has all required variables.

### Memory Issues
Adjust batch sizes in `src/config/config.py`.

## 📞 Support

For issues or questions, please refer to the project repository.

---

**Version**: 2.0.0  
**Last Updated**: December 2025
