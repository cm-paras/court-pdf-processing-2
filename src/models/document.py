"""Document and chunk data models."""

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum

class DocumentStatus(Enum):
    """Document processing status."""
    PENDING = "pending"
    DOWNLOAD_FAILED = "download_failed"
    OCR_FAILED = "ocr_failed"
    METADATA_FAILED = "metadata_failed"
    CHUNKING_FAILED = "chunking_failed"
    EMBEDDING_FAILED = "embedding_failed"
    INDEXED = "indexed"

class ChunkStatus(Enum):
    """Chunk processing status."""
    PENDING_EMBEDDING = "pending_embedding"
    EMBEDDED = "embedded"
    FAILED_EMBEDDING = "failed_embedding"
    INDEXED = "indexed"

@dataclass
class DocumentMetadata:
    """Extracted document metadata."""
    case_name: str = ""
    case_number: str = ""
    citation: str = ""
    date_of_judgment: str = ""
    bench: str = ""
    court: str = ""
    summary: str = ""
    keywords: List[str] = None
    petitioner_advocates: List[str] = None
    respondent_advocates: List[str] = None
    
    def __post_init__(self):
        if self.keywords is None:
            self.keywords = []
        if self.petitioner_advocates is None:
            self.petitioner_advocates = []
        if self.respondent_advocates is None:
            self.respondent_advocates = []
    
    def is_valid(self) -> bool:
        """Check if metadata meets minimum requirements."""
        has_case_info = bool(self.case_name.strip() or self.case_number.strip())
        has_court = bool(self.court.strip())
        has_valid_date = self._is_valid_date(self.date_of_judgment)
        
        return has_case_info and has_court and has_valid_date
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date format YYYY-MM-DD."""
        if not date_str:
            return False
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

@dataclass
class ProcessingTimestamps:
    """Processing stage timestamps."""
    download_start: Optional[datetime] = None
    download_end: Optional[datetime] = None
    ocr_start: Optional[datetime] = None
    ocr_end: Optional[datetime] = None
    metadata_start: Optional[datetime] = None
    metadata_end: Optional[datetime] = None
    chunking_start: Optional[datetime] = None
    chunking_end: Optional[datetime] = None
    embedding_start: Optional[datetime] = None
    embedding_end: Optional[datetime] = None
    indexing_start: Optional[datetime] = None
    indexing_end: Optional[datetime] = None

@dataclass
class Document:
    """Main document record."""
    pdf_id: str
    blob_url: str
    file_size_bytes: int = 0
    full_text: str = ""
    per_page_texts: List[str] = None
    ocr_confidence: float = 0.0
    metadata_json: Dict[str, Any] = None
    status: DocumentStatus = DocumentStatus.PENDING
    processing_timestamps: ProcessingTimestamps = None
    error_message: str = ""
    created_at: datetime = None
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.per_page_texts is None:
            self.per_page_texts = []
        if self.metadata_json is None:
            self.metadata_json = {}
        if self.processing_timestamps is None:
            self.processing_timestamps = ProcessingTimestamps()
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Cosmos DB."""
        data = asdict(self)
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Document':
        """Create from Cosmos DB dictionary."""
        if 'status' in data:
            data['status'] = DocumentStatus(data['status'])
        return cls(**data)

@dataclass
class Chunk:
    """Document chunk record."""
    chunk_id: str
    pdf_id: str
    text: str
    metadata: Dict[str, Any]
    embedding_vector: List[float] = None
    chunk_index: int = 0
    chunk_total: int = 0
    status: ChunkStatus = ChunkStatus.PENDING_EMBEDDING
    embedding_attempts: int = 0
    created_at: datetime = None
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.embedding_vector is None:
            self.embedding_vector = []
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Cosmos DB."""
        data = asdict(self)
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Chunk':
        """Create from Cosmos DB dictionary."""
        if 'status' in data:
            data['status'] = ChunkStatus(data['status'])
        return cls(**data)