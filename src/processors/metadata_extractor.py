"""
Metadata extraction from legal documents using Azure OpenAI
"""
import json
import time
import hashlib
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils import get_logger

logger = get_logger(__name__)


def hash_text(text):
    """Create a deterministic hash of text for caching"""
    if not text:
        return "empty"
    return hashlib.md5(text.encode('utf-8')).hexdigest()


class MetadataExtractor:
    """Handles metadata extraction from legal documents"""
    
    def __init__(self, azure_clients, config):
        """
        Initialize metadata extractor
        
        Args:
            azure_clients: AzureClientManager instance
            config: Configuration object
        """
        self.openai_client = azure_clients.openai_client
        self.config = config
    
    def extract_metadata(self, text, text_hash=None):
        """
        Extract metadata from text using Azure OpenAI
        
        Args:
            text: Document text
            text_hash: Optional hash for caching
            
        Returns:
            dict: Extracted metadata
        """
        if not self.config.AZURE_OPENAI_CHAT_MODEL:
            logger.warning("No chat model configured, skipping metadata extraction")
            return self._create_minimal_metadata(text)
        
        try:
            text_preview = text[:15000] if len(text) > 15000 else text
            
            system_prompt = """You are a legal document analyzer specialized in Indian High Court judgments.
Extract metadata from legal documents and return ONLY a valid JSON object with the specified fields.
If you cannot find a particular field, use null or an empty string.
For "Petitioner Advocates" and "Respondent Advocates", always provide values as simple strings, not arrays or lists."""
            
            user_prompt = f"""Extract the following metadata from this legal document:

1. Case Name: Extract the full case name with all parties
2. Citation: Extract the formal citation if available
3. Case Number: Extract the case number/petition number
4. Date of Judgment: Extract the judgment date (format: YYYY-MM-DD)
5. Bench: Extract the name(s) of judge(s) who delivered the judgment
6. Subject Matter: Identify the primary legal subject
7. Keywords: Identify 5-10 key legal terms or concepts from the judgment
8. Summary: Provide a brief (3-5 sentences) summary of the case
9. Petitioner Advocates: Extract names of advocates representing the petitioner
10. Respondent Advocates: Extract names of advocates representing the respondent
11. Court: Extract the name of the court

Document text:
{text_preview}"""
            
            response = self._call_openai_with_retry(system_prompt, user_prompt)
            
            if not response or not response.choices:
                raise ValueError("No response from Azure OpenAI API")
            
            metadata = self._parse_json_response(response.choices[0].message.content.strip())
            
            # Validate and add extra fields
            metadata = self._validate_metadata(metadata, text)
            
            return metadata
        
        except Exception as e:
            logger.error(f"Error extracting metadata with Azure OpenAI: {e}")
            return self._create_error_metadata(text, str(e))
    
    def _call_openai_with_retry(self, system_prompt, user_prompt):
        """Call OpenAI API with retry logic"""
        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = self.openai_client.chat.completions.create(
                    model=self.config.AZURE_OPENAI_CHAT_MODEL,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.1,
                    max_tokens=2048,
                    response_format={"type": "json_object"}
                )
                return response
            except Exception as e:
                if attempt < self.config.MAX_RETRIES - 1:
                    logger.warning(
                        f"Azure OpenAI API call failed, retrying ({attempt+1}/{self.config.MAX_RETRIES}): {e}"
                    )
                    time.sleep(self.config.RETRY_DELAY * (2 ** attempt))
                else:
                    logger.error(f"Azure OpenAI API call failed after {self.config.MAX_RETRIES} attempts: {e}")
                    raise
    
    def _parse_json_response(self, metadata_json):
        """Parse JSON response from OpenAI"""
        try:
            return json.loads(metadata_json)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            # Try to extract JSON using regex
            import re
            json_pattern = r'\{.*\}'
            json_match = re.search(json_pattern, metadata_json, re.DOTALL)
            
            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except:
                    logger.warning(f"Failed to extract JSON with regex from: {metadata_json[:100]}...")
                    raise ValueError("Invalid JSON response from Azure OpenAI")
            else:
                raise ValueError("No JSON-like structure found in Azure OpenAI response")
    
    def _validate_metadata(self, metadata, text):
        """Validate and fill in missing metadata fields"""
        required_fields = [
            "Case Name", "Citation", "Case Number", "Date of Judgment",
            "Bench", "Subject Matter", "Keywords", "Summary",
            "Petitioner Advocates", "Respondent Advocates", "Court"
        ]
        
        for field in required_fields:
            if field not in metadata:
                metadata[field] = None
        
        metadata["text_length"] = len(text)
        metadata["processing_timestamp"] = time.time()
        
        return metadata
    
    def _create_minimal_metadata(self, text):
        """Create minimal metadata when extraction is skipped"""
        return {
            "Case Name": "Unknown",
            "Citation": "Unknown",
            "Case Number": "Unknown",
            "Date of Judgment": None,
            "Bench": None,
            "Subject Matter": None,
            "Keywords": [],
            "Summary": "Metadata extraction skipped",
            "Petitioner Advocates": None,
            "Respondent Advocates": None,
            "Court": None,
            "text_length": len(text) if text else 0,
            "processing_timestamp": time.time()
        }
    
    def _create_error_metadata(self, text, error_msg):
        """Create error metadata when extraction fails"""
        return {
            "Case Name": "Unknown",
            "Citation": "Unknown",
            "Case Number": "Unknown",
            "Date of Judgment": None,
            "Bench": None,
            "Subject Matter": None,
            "Keywords": [],
            "Summary": "Error extracting metadata",
            "Petitioner Advocates": None,
            "Respondent Advocates": None,
            "Court": None,
            "text_length": len(text) if text else 0,
            "processing_timestamp": time.time(),
            "error": error_msg
        }
    
    def extract_batch(self, texts_dict):
        """
        Extract metadata from multiple documents in parallel with rate limiting
        
        Args:
            texts_dict: Dictionary mapping blob names to text
            
        Returns:
            dict: Mapping of blob names to metadata
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=min(len(texts_dict), 2)) as executor:
            futures_map = {}
            for i, (blob_name, text) in enumerate(texts_dict.items()):
                if text:
                    # Rate limit: 1 second delay between submissions
                    if i > 0:
                        time.sleep(1)
                    
                    text_hash = hash_text(text[:15000])
                    future = executor.submit(self.extract_metadata, text, text_hash)
                    futures_map[future] = blob_name
            
            for future in as_completed(futures_map):
                blob_name = futures_map[future]
                try:
                    metadata = future.result()
                    results[blob_name] = metadata
                except Exception as e:
                    logger.error(f"Error extracting metadata for {blob_name}: {str(e)}")
                    results[blob_name] = self._create_error_metadata(
                        texts_dict.get(blob_name, ""),
                        str(e)
                    )
        
        return results
