"""OCR processor with quality validation."""

import logging
import fitz  # PyMuPDF
from typing import Tuple, List
from datetime import datetime

from ..config.settings import CONFIG
from ..models.document import Document, DocumentStatus
from ..utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

class OCRProcessor:
    """Extracts text from PDFs with quality validation."""
    
    @retry_with_backoff()
    def extract_text(self, document: Document, pdf_content: bytes) -> bool:
        """Extract full text from PDF with validation."""
        document.processing_timestamps.ocr_start = datetime.utcnow()
        
        try:
            logger.info(f"Starting OCR for PDF {document.pdf_id}")
            
            # Open PDF
            pdf_doc = fitz.open(stream=pdf_content, filetype="pdf")
            
            full_text = ""
            per_page_texts = []
            total_confidence = 0.0
            page_count = 0
            
            for page_num in range(len(pdf_doc)):
                page = pdf_doc[page_num]
                
                # Extract text
                page_text = page.get_text()
                per_page_texts.append(page_text)
                full_text += page_text + "\n"
                
                # Calculate confidence (basic heuristic)
                page_confidence = self._calculate_page_confidence(page, page_text)
                total_confidence += page_confidence
                page_count += 1
            
            pdf_doc.close()
            
            # Calculate overall confidence
            avg_confidence = total_confidence / page_count if page_count > 0 else 0.0
            
            # Validate OCR results
            if not self._validate_ocr_results(full_text, avg_confidence):
                document.status = DocumentStatus.OCR_FAILED
                document.error_message = f"OCR validation failed: text_len={len(full_text)}, confidence={avg_confidence:.2f}"
                document.processing_timestamps.ocr_end = datetime.utcnow()
                return False
            
            # Store results
            document.full_text = full_text.strip()
            document.per_page_texts = per_page_texts
            document.ocr_confidence = avg_confidence
            document.processing_timestamps.ocr_end = datetime.utcnow()
            
            logger.info(f"OCR completed for PDF {document.pdf_id}: {len(full_text)} chars, confidence={avg_confidence:.2f}")
            return True
            
        except Exception as e:
            document.status = DocumentStatus.OCR_FAILED
            document.error_message = f"OCR failed: {str(e)}"
            document.processing_timestamps.ocr_end = datetime.utcnow()
            logger.error(f"OCR failed for PDF {document.pdf_id}: {e}")
            return False
    
    def _calculate_page_confidence(self, page, text: str) -> float:
        """Calculate OCR confidence for a page."""
        if not text.strip():
            return 0.0
        
        # Basic heuristics for confidence
        char_count = len(text)
        word_count = len(text.split())
        
        if word_count == 0:
            return 0.0
        
        # Check for reasonable character-to-word ratio
        avg_word_length = char_count / word_count
        if avg_word_length < 2 or avg_word_length > 20:
            return 0.3
        
        # Check for excessive special characters
        special_char_ratio = sum(1 for c in text if not c.isalnum() and not c.isspace()) / char_count
        if special_char_ratio > 0.3:
            return 0.4
        
        # Check for reasonable uppercase/lowercase distribution
        alpha_chars = [c for c in text if c.isalpha()]
        if alpha_chars:
            upper_ratio = sum(1 for c in alpha_chars if c.isupper()) / len(alpha_chars)
            if upper_ratio > 0.8 or upper_ratio < 0.01:
                return 0.5
        
        return 0.8  # Good confidence
    
    def _validate_ocr_results(self, text: str, confidence: float) -> bool:
        """Validate OCR results against quality thresholds."""
        # Check text length
        if len(text.strip()) < CONFIG.OCR_MIN_TEXT_LENGTH:
            logger.warning(f"Text too short: {len(text)} < {CONFIG.OCR_MIN_TEXT_LENGTH}")
            return False
        
        # Check confidence
        if confidence < CONFIG.OCR_MIN_CONFIDENCE:
            logger.warning(f"Confidence too low: {confidence} < {CONFIG.OCR_MIN_CONFIDENCE}")
            return False
        
        # Check for empty text
        if not text.strip():
            logger.warning("Empty text extracted")
            return False
        
        return True