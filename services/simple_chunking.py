import logging
from typing import List, Dict, Any
import re

logger = logging.getLogger(__name__)

class SimpleChunking:
    def __init__(self):
        self.base_chunk_size = 1200
        self.base_chunk_overlap = 120
    
    def chunk_text(self, text: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Ultra-optimized chunking for maximum performance"""
        try:
            # Dynamic chunk sizing based on document size
            text_length = len(text)
            if text_length > 2000000:  # Over 2MB of text
                chunk_size = 3500
                chunk_overlap = 300
                logger.info(f"Using extra large file chunking: {chunk_size} chars")
            elif text_length > 1000000:  # Over 1MB of text
                chunk_size = 2500
                chunk_overlap = 250
                logger.info(f"Using large file chunking: {chunk_size} chars")
            elif text_length > 500000:  # Over 500KB of text
                chunk_size = 1800  # Increased from 1500
                chunk_overlap = 180
                logger.info(f"Using medium file chunking: {chunk_size} chars")
            else:
                chunk_size = self.base_chunk_size
                chunk_overlap = self.base_chunk_overlap
            
            return self._fast_chunking(text, metadata, chunk_size, chunk_overlap)
            
        except Exception as e:
            logger.error(f"Chunking error: {str(e)}")
            return self._fallback_chunking(text, metadata)

    def _fast_chunking(self, text: str, metadata: Dict[str, Any], chunk_size: int, chunk_overlap: int) -> List[Dict[str, Any]]:
        """Fast chunking with minimal processing"""
        text = self._clean_text(text)
        
        if not text or len(text.strip()) < 50:
            return [{"text": text[:4000], "metadata": metadata.copy()}]

        # Simple word-based chunking for maximum speed
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            word_length = len(word) + 1  # +1 for space
            
            if current_length + word_length > chunk_size and current_chunk:
                # Save current chunk
                chunk_text = " ".join(current_chunk)
                chunks.append({
                    "text": chunk_text,
                    "metadata": metadata.copy()
                })
                
                # Keep overlap words for next chunk
                overlap_count = min(len(current_chunk), chunk_overlap // 10)
                current_chunk = current_chunk[-overlap_count:] if overlap_count > 0 else []
                current_length = sum(len(w) + 1 for w in current_chunk)
                
                # Add current word
                current_chunk.append(word)
                current_length += word_length
            else:
                current_chunk.append(word)
                current_length += word_length
        
        # Add the last chunk
        if current_chunk:
            chunk_text = " ".join(current_chunk)
            chunks.append({
                "text": chunk_text,
                "metadata": metadata.copy()
            })
        
        logger.info(f"Fast chunking created {len(chunks)} chunks with size {chunk_size}")
        return chunks

    def _clean_text(self, text: str) -> str:
        """Minimal text cleaning for speed"""
        # Only remove excessive spaces, preserve everything else
        text = re.sub(r' +', ' ', text)
        return text.strip()

# Global instance
simple_chunking = SimpleChunking()