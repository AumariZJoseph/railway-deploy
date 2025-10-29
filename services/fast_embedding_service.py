import logging
import asyncio
from typing import List
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FastEmbeddingService:
    def __init__(self):
        # Switch to smaller model - 80MB vs 130MB
        self.model_name = "sentence-transformers/all-MiniLM-L6-v2"
        self.dimension = 384
        self._model = None
        self._model_loaded = False
        logger.info(f"FastEmbeddingService initialized - will use {self.model_name}")

    def _ensure_model_loaded(self):
        """Lazy load model only when needed"""
        if self._model_loaded:
            return
            
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Lazy loading model: {self.model_name}")
            self._model = SentenceTransformer(self.model_name)
            self._model.max_seq_length = 512
            self._model_loaded = True
            logger.info(f"Model {self.model_name} loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise

    async def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Batch embedding generation with lazy loading"""
        if not texts:
            return []

        try:
            # Lazy load model on first use
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._ensure_model_loaded)
            
            processed_texts = [' '.join(text.split()[:500]) for text in texts]
            
            embeddings = await loop.run_in_executor(
                None,
                self._encode_batch_quality,
                processed_texts
            )
            
            logger.info(f"Generated {len(embeddings)} embeddings with {self.model_name}")
            return embeddings
            
        except Exception as e:
            logger.error(f"Embedding error: {str(e)}")
            return [[0.1] * self.dimension for _ in texts]

    def _encode_batch_quality(self, texts: List[str]) -> List[List[float]]:
        """Quality-focused encoding"""
        embeddings = self._model.encode(
            texts,
            show_progress_bar=False,
            normalize_embeddings=True,
            batch_size=min(len(texts), 256),
            convert_to_numpy=True,
            convert_to_tensor=False,
            device='cpu'
        )
        
        return embeddings.tolist()

    async def get_embedding(self, text: str) -> List[float]:
        """Get single embedding with lazy loading"""
        embeddings = await self.get_embeddings_batch([text])
        return embeddings[0] if embeddings else [0.1] * self.dimension

# Global instance
fast_embedding_service = FastEmbeddingService()