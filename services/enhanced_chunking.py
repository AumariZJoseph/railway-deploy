import logging
from typing import List, Dict, Any
import re

logger = logging.getLogger(__name__)

class EnhancedChunking:
    def __init__(self):
        self.base_chunk_size = 1000  # Slightly smaller for better context
        self.base_chunk_overlap = 150
    
    def chunk_text(self, text: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Enhanced chunking that preserves document structure better"""
        try:
            # Clean and structure the text first
            cleaned_text = self._clean_and_structure_text(text, metadata.get('source', ''))
            
            # Dynamic chunk sizing based on content type
            text_length = len(cleaned_text)
            if text_length > 1000000:  # Large documents
                chunk_size = 1200
                chunk_overlap = 200
            elif text_length > 100000:  # Medium documents
                chunk_size = 1000
                chunk_overlap = 150
            else:  # Small documents
                chunk_size = 800  # Smaller chunks for small files
                chunk_overlap = 100
            
            return self._semantic_chunking(cleaned_text, metadata, chunk_size, chunk_overlap)
            
        except Exception as e:
            logger.error(f"Enhanced chunking error: {str(e)}")
            return self._fallback_chunking(text, metadata)

    def _clean_and_structure_text(self, text: str, source: str) -> str:
        """Clean and add structure to text like old system"""
        # Remove excessive whitespace but preserve structure
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        # Add document header like old system
        if source:
            structured_text = f"DOCUMENT: {source}\nCONTENT:\n{text}"
        else:
            structured_text = text
            
        return structured_text.strip()

    def _semantic_chunking(self, text: str, metadata: Dict[str, Any], chunk_size: int, chunk_overlap: int) -> List[Dict[str, Any]]:
        """Chunking that preserves semantic boundaries"""
        # Split by paragraphs/sections first
        paragraphs = re.split(r'\n\s*\n', text)
        chunks = []
        current_chunk = ""
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue
                
            # If adding this paragraph exceeds chunk size, save current chunk
            if len(current_chunk) + len(paragraph) > chunk_size and current_chunk:
                chunks.append({
                    "text": current_chunk.strip(),
                    "metadata": metadata.copy()
                })
                
                # Keep overlap from previous chunk
                overlap_text = self._get_overlap_text(current_chunk, chunk_overlap)
                current_chunk = overlap_text + "\n\n" + paragraph if overlap_text else paragraph
            else:
                if current_chunk:
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph
        
        # Add the last chunk
        if current_chunk:
            chunks.append({
                "text": current_chunk.strip(),
                "metadata": metadata.copy()
            })
        
        logger.info(f"Enhanced chunking created {len(chunks)} chunks")
        return chunks

    def _get_overlap_text(self, text: str, overlap_size: int) -> str:
        """Get overlap text from the end of a chunk"""
        words = text.split()
        if len(words) <= overlap_size // 5:  # Rough word count estimate
            return text
        
        # Take last few sentences for overlap
        sentences = re.split(r'[.!?]+', text)
        overlap_sentences = []
        current_length = 0
        
        for sentence in reversed(sentences):
            sentence = sentence.strip()
            if not sentence:
                continue
            if current_length + len(sentence) > overlap_size and overlap_sentences:
                break
            overlap_sentences.insert(0, sentence)
            current_length += len(sentence)
        
        return '. '.join(overlap_sentences) + '.'

    def _fallback_chunking(self, text: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fallback to word-based chunking"""
        words = text.split()
        chunk_size_words = 200  # Approximate words
        chunks = []
        
        for i in range(0, len(words), chunk_size_words):
            chunk_text = ' '.join(words[i:i + chunk_size_words])
            chunks.append({
                "text": chunk_text,
                "metadata": metadata.copy()
            })
        
        return chunks

# Global instance
enhanced_chunking = EnhancedChunking()