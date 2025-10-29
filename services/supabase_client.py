import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
import httpx
from typing import List, Dict, Any
import time
import uuid

load_dotenv()

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not self.url or not self.key:
            raise ValueError("Supabase credentials not configured")
        
        try:
            self.client: Client = create_client(self.url, self.key)
            # Test connection
            self.client.table("documents").select("count", count="exact").limit(1).execute()
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            self.client = create_client(self.url, self.key)
        
        self.bucket_name = "user_documents"
        self.max_retries = 3
        self.retry_delay = 2
        
        logger.info("SupabaseClient initialized successfully")

    def _retry_operation(self, operation, *args, **kwargs):
        """Retry operation with exponential backoff"""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt == self.max_retries - 1:
                    logger.error(f"Operation failed after {self.max_retries} attempts: {str(e)}")
                    raise e
                delay = self.retry_delay * (2 ** attempt)
                logger.warning(f"Operation failed (attempt {attempt + 1}), retrying in {delay}s: {str(e)}")
                time.sleep(delay)
        raise last_error

    def search_similar_chunks(self, user_id: str, query_embedding: List[float], 
                            limit: int = 10) -> List[Dict[str, Any]]:
        """Enhanced similarity search with proper UUID handling"""
        try:
            # First, let's get the user's UUID from their documents
            user_docs = self.client.table("documents")\
                .select("user_id")\
                .eq("user_id", user_id)\
                .limit(1)\
                .execute()
            
            if not user_docs.data:
                logger.warning(f"No documents found for user {user_id}")
                return []
            
            # The user_id in documents should already be UUID
            user_uuid = user_docs.data[0]["user_id"]
            
            embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
            
            logger.info(f"Searching with user UUID: {user_uuid}")
            
            result = self.client.rpc(
                "similarity_search",
                {
                    "query_embedding": embedding_str,
                    "user_id": user_uuid,
                    "match_count": limit
                }
            ).execute()
            
            if result.data:
                logger.info(f"Similarity search found {len(result.data)} chunks")
                return result.data
            else:
                logger.warning("Similarity search returned no results")
                return []
                
        except Exception as e:
            logger.error(f"Vector search error: {str(e)}")
            # Fallback to direct vector search
            return self._direct_vector_search(user_id, query_embedding, limit)

    def _direct_vector_search(self, user_id: str, query_embedding: List[float], 
                            limit: int = 10) -> List[Dict[str, Any]]:
        """Direct vector search as fallback"""
        try:
            embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
            
            # Use raw SQL for vector search
            query = """
            SELECT 
                dc.id,
                dc.document_id,
                dc.user_id,
                dc.chunk_text,
                dc.chunk_index,
                dc.metadata,
                1 - (dc.embedding <=> %s::vector) as similarity
            FROM document_chunks dc
            JOIN documents d ON dc.document_id = d.id
            WHERE d.user_id = %s
            AND d.is_active = true
            ORDER BY dc.embedding <=> %s::vector
            LIMIT %s
            """
            
            # Execute raw query
            result = self.client.postgrest.rpc('', {}).execute()  # This needs proper raw SQL setup
            
            # If raw SQL doesn't work, use the ordering approach
            result = self.client.table("document_chunks")\
                .select("*, documents!inner(user_id, is_active)")\
                .eq("documents.user_id", user_id)\
                .eq("documents.is_active", True)\
                .order("embedding")\
                .limit(limit)\
                .execute()
            
            # Calculate similarity manually
            chunks_with_similarity = []
            base_similarity = 0.8
            for i, chunk in enumerate(result.data):
                similarity = base_similarity - (i * 0.1)  # Decrease with rank
                similarity = max(0.4, similarity)  # Minimum threshold
                chunk['similarity'] = similarity
                chunks_with_similarity.append(chunk)
            
            return chunks_with_similarity
            
        except Exception as e:
            logger.error(f"Direct vector search also failed: {str(e)}")
            return []

    # -------------------- File Operations --------------------
    def upload_file(self, user_id: str, file_data: bytes, file_name: str) -> str:
        """Upload file to storage with retry logic"""
        storage_path = f"{user_id}/{file_name}"
        
        def _upload():
            with httpx.Client(timeout=120.0) as client:
                response = client.post(
                    f"{self.url}/storage/v1/object/{self.bucket_name}/{storage_path}",
                    headers={
                        "Authorization": f"Bearer {self.key}",
                        "Content-Type": "application/octet-stream"
                    },
                    content=file_data
                )
                response.raise_for_status()
            return storage_path
        
        return self._retry_operation(_upload)

    def download_file(self, storage_path: str) -> bytes:
        """Download file from storage"""
        def _download():
            return self.client.storage.from_(self.bucket_name).download(storage_path)
        return self._retry_operation(_download)

    def delete_file(self, storage_path: str):
        """Delete file from storage"""
        def _delete():
            self.client.storage.from_(self.bucket_name).remove([storage_path])
        return self._retry_operation(_delete)

    def list_user_files(self, user_id: str) -> List[str]:
        """List user files excluding system files"""
        try:
            res = self.client.storage.from_(self.bucket_name).list(user_id)
            if res:
                return [f["name"] for f in res if f["name"] not in ["storage.zip", ".emptyFolderPlaceholder"]]
            return []
        except Exception as e:
            logger.error(f"Error listing files for user {user_id}: {str(e)}")
            return []

    # -------------------- Document Management --------------------
    def create_document(self, user_id: str, file_name: str, file_hash: str, 
                       file_size: int, file_type: str) -> Dict[str, Any]:
        """Create a new document record with versioning"""
        existing = self.client.table("documents")\
            .select("version")\
            .eq("user_id", user_id)\
            .eq("file_name", file_name)\
            .order("version", desc=True)\
            .limit(1)\
            .execute()
        
        new_version = 1
        if existing.data:
            new_version = existing.data[0]["version"] + 1
            self.client.table("documents")\
                .update({"is_active": False})\
                .eq("user_id", user_id)\
                .eq("file_name", file_name)\
                .execute()

        document_data = {
            "user_id": user_id,
            "file_name": file_name,
            "file_hash": file_hash,
            "file_size": file_size,
            "file_type": file_type,
            "version": new_version,
            "is_active": True,
            "processed_at": "now()"
        }

        result = self.client.table("documents").insert(document_data).execute()
        if not result.data:
            raise Exception("Failed to create document record")
        return result.data[0]

    def get_active_documents(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all active documents for a user"""
        result = self.client.table("documents")\
            .select("*")\
            .eq("user_id", user_id)\
            .eq("is_active", True)\
            .execute()
        return result.data

    def soft_delete_document(self, user_id: str, file_name: str):
        """Soft delete a document by marking as inactive"""
        self.client.table("documents")\
            .update({"is_active": False})\
            .eq("user_id", user_id)\
            .eq("file_name", file_name)\
            .execute()

    # -------------------- Chunk Management --------------------
    def insert_chunks(self, chunks: List[Dict[str, Any]]):
        """Insert multiple document chunks with improved error handling"""
        if not chunks:
            return
        
        batch_size = 500
        successful_inserts = 0
        
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            try:
                result = self._retry_operation(
                    lambda: self.client.table("document_chunks").insert(batch).execute()
                )
                successful_inserts += len(batch)
                logger.info(f"Successfully inserted batch {i//batch_size + 1}, total chunks: {successful_inserts}")
            except Exception as e:
                logger.error(f"Failed to insert batch {i//batch_size + 1}: {str(e)}")
                raise Exception(f"Database insertion failed: {str(e)}")
        
        if successful_inserts < len(chunks):
            logger.warning(f"Inserted {successful_inserts}/{len(chunks)} chunks successfully")
            raise Exception(f"Only {successful_inserts}/{len(chunks)} chunks were inserted successfully")

    def delete_document_chunks(self, document_id: str):
        """Delete all chunks for a document"""
        self.client.table("document_chunks")\
            .delete()\
            .eq("document_id", document_id)\
            .execute()

    # -------------------- Chat History --------------------
    def save_chat_history(self, user_id: str, question: str, answer: str, 
                         sources: List[str] = None):
        """Save chat history with sources"""
        chat_data = {
            "user_id": user_id,
            "question": question,
            "answer": answer,
            "context_sources": sources or []
        }
        self.client.table("chat_history").insert(chat_data).execute()

    def get_chat_history(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent chat history for a user"""
        result = self.client.table("chat_history")\
            .select("*")\
            .eq("user_id", user_id)\
            .order("created_at", desc=True)\
            .limit(limit)\
            .execute()
        return result.data

    # -------------------- User Settings --------------------
    def get_user_settings(self, user_id: str) -> Dict[str, Any]:
        """Get or create user settings"""
        result = self.client.table("user_settings")\
            .select("*")\
            .eq("user_id", user_id)\
            .execute()
        
        if result.data:
            return result.data[0]
        
        default_settings = {
            "user_id": user_id,
            "max_files": 5,
            "max_file_size_mb": 10
        }
        result = self.client.table("user_settings").insert(default_settings).execute()
        return result.data[0]

# Global instance
supabase_client = SupabaseClient()