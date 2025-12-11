"""Metadata extraction using Azure OpenAI."""

import logging
import json
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class MetadataExtractor:
    """Extracts structured metadata from legal documents using Azure OpenAI."""
    
    def __init__(self, azure_clients, config):
        self.openai_client = azure_clients.openai_client
        self.config = config
    
    def extract_batch(self, texts_dict):
        """Extract metadata from multiple documents."""
        metadata_dict = {}
        
        for blob_name, text in texts_dict.items():
            try:
                logger.info(f"Extracting metadata for {blob_name}")
                
                prompt = self._build_extraction_prompt(text)
                
                response = self.openai_client.chat.completions.create(
                    model=self.config.AZURE_OPENAI_CHAT_MODEL,
                    messages=[
                        {"role": "system", "content": "You are a legal document analyzer. Extract metadata and return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=1000
                )
                
                if not response.choices[0].message.content:
                    raise ValueError("Empty response from OpenAI")
                
                # Parse JSON response
                content = response.choices[0].message.content.strip()
                if content.startswith('```json'):
                    content = content[7:-3]
                elif content.startswith('```'):
                    content = content[3:-3]
                
                metadata = json.loads(content)
                
                # Basic validation
                if not metadata.get('case_name') and not metadata.get('case_number'):
                    logger.warning(f"Invalid metadata for {blob_name}: missing case_name and case_number")
                    continue
                
                metadata_dict[blob_name] = metadata
                logger.info(f"Metadata extracted for {blob_name}: {metadata.get('case_name', 'Unknown')}")
                
            except Exception as e:
                logger.error(f"Metadata extraction failed for {blob_name}: {e}")
                continue
        
        return metadata_dict
    
    def _build_extraction_prompt(self, text: str) -> str:
        """Build prompt for metadata extraction."""
        return f"""
Extract structured metadata from this legal judgment text. Return ONLY valid JSON with these exact fields:

{{
    "case_name": "Full case name",
    "case_number": "Case number/citation",
    "citation": "Legal citation",
    "date_of_judgment": "YYYY-MM-DD format",
    "bench": "Judge names",
    "court": "Court name",
    "summary": "Brief case summary",
    "keywords": ["keyword1", "keyword2"],
    "petitioner_advocates": ["advocate1", "advocate2"],
    "respondent_advocates": ["advocate1", "advocate2"]
}}

Requirements:
- case_name OR case_number must not be empty
- court must not be empty  
- date_of_judgment must be valid YYYY-MM-DD format
- All string fields should be properly escaped for JSON
- Arrays can be empty but must exist

Text to analyze:
{text[:50000]}
"""