import os
import logging
import tempfile
import asyncio
from pathlib import Path
from services.task_queue import background_queue

from services.supabase_client import supabase_client
from services.ingest_utils import process_file, get_file_metadata
from services.enhanced_chunking import enhanced_chunking
from services.fast_embedding_service import fast_embedding_service
from services.error_handler import (
    ValidationError, 
    ExternalServiceError, 
    ResourceNotFoundError,
    log_error
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FastIngestService:
    def __init__(self):
        self.max_files = 5
        logger.info("FastIngestService initialized with optimized settings")

    async def ingest_file(self, user_id: str, file) -> dict:
        """Fast file ingestion with comprehensive error handling"""
        try:
            # Validate inputs
            if not user_id or not isinstance(user_id, str):
                raise ValidationError("Invalid user ID", "Invalid user identifier.")
            
            if not file or not hasattr(file, 'filename'):
                raise ValidationError("Invalid file", "No file provided or file is invalid.")

            # Check file limit
            try:
                await self._check_file_limit(user_id)
            except Exception as e:
                raise ValidationError(str(e), str(e))

            # Read file content
            content = await file.read()
            file_name = file.filename
            
            logger.info(f"Starting ingestion for {file_name} ({len(content)} bytes)")
            
            # Create temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_name) as temp_file:
                temp_file.write(content)
                temp_path = temp_file.name

            result = await self._ingest_file_sync_internal(user_id, temp_path, file_name, temp_path)
            return result

        except (ValidationError, ExternalServiceError, ResourceNotFoundError):
            raise
        except Exception as e:
            log_error(e, user_id, "ingest_file", {"filename": file.filename if file else "unknown"})
            raise ExternalServiceError(
                f"Unexpected ingestion error: {str(e)}",
                "An unexpected error occurred during file ingestion. Please try again."
            )

    async def _ingest_file_sync_internal(self, user_id: str, file_path: str, filename: str, temp_path: str) -> dict:
        """Internal async method for background or direct processing"""
        try:
            loop = asyncio.get_event_loop()

            # Step 1: Upload file
            logger.info(f"Uploading {filename} to storage...")
            try:
                storage_path = await loop.run_in_executor(
                    None, supabase_client.upload_file, user_id, open(file_path, "rb").read(), filename
                )
                logger.info(f"File uploaded: {storage_path}")
            except Exception as e:
                raise ExternalServiceError(
                    f"File storage upload failed: {str(e)}",
                    "Failed to upload file to storage. Please try again."
                )

            # Step 2: Process file content
            logger.info(f"Processing file content for {filename}...")
            try:
                text_content, error = await loop.run_in_executor(None, process_file, temp_path, filename)
                if error:
                    raise ValidationError(f"File processing failed: {error}", "Unsupported or corrupted file.")
            except Exception as e:
                raise ExternalServiceError(f"File processing error: {str(e)}", "Error processing file content.")

            # Step 3: Create document record
            try:
                file_meta = get_file_metadata(temp_path)
                document = await loop.run_in_executor(
                    None, supabase_client.create_document,
                    user_id, filename, file_meta["hash"], file_meta["size"], Path(filename).suffix[1:]
                )
            except Exception as e:
                raise ExternalServiceError(f"Document record creation failed: {str(e)}", "Failed to create record.")

            # Step 4: Chunk text
            logger.info(f"Chunking content for {filename}...")
            try:
                chunks = enhanced_chunking.chunk_text(text_content, {
                    "source": filename,
                    "file_type": Path(filename).suffix[1:],
                    "user_id": user_id,
                    "document_id": document["id"]
                })
                logger.info(f"Created {len(chunks)} chunks")
            except Exception as e:
                raise ExternalServiceError(f"Chunking failed: {str(e)}", "Error splitting document content.")

            # Step 5: Process chunks
            await self._process_chunks_optimized(document["id"], user_id, chunks)
            logger.info(f"Ingestion complete: {filename}")

            return {
                "status": "success",
                "document_id": document["id"],
                "chunks_processed": len(chunks)
            }

        finally:
            try:
                os.unlink(temp_path)
            except Exception as e:
                logger.warning(f"Failed to delete temp file {temp_path}: {str(e)}")

    async def ingest_file_background(self, user_id: str, file) -> dict:
        """Submit file ingestion to background queue and return immediately"""
        try:
            if not user_id or not hasattr(file, 'filename'):
                raise ValidationError("Invalid file or user ID", "Invalid parameters for background task.")

            content = await file.read()
            file_name = file.filename

            # Create temp file quickly
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_name) as temp_file:
                temp_file.write(content)
                temp_path = temp_file.name

            # Submit background job
            task_id = background_queue.submit_task(
                self._process_file_sync,
                user_id, temp_path, file_name, temp_path
            )

            return {
                "status": "processing",
                "task_id": task_id,
                "message": f"File {file_name} is being processed in the background"
            }

        except Exception as e:
            logger.error(f"Failed to submit background task: {str(e)}")
            raise ExternalServiceError(
                f"Failed to start background processing: {str(e)}",
                "Unable to process file in background. Please try again."
            )

    def _process_file_sync(self, user_id: str, file_path: str, filename: str, temp_path: str) -> dict:
        """Synchronous file processing for background tasks"""
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    self._ingest_file_sync_internal(user_id, file_path, filename, temp_path)
                )
                return result
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Background file processing failed: {str(e)}")
            raise

    async def _process_chunks_optimized(self, document_id: str, user_id: str, chunks: list):
        """Optimized sequential chunk processing with better error handling"""
        if not chunks:
            return

        try:
            batch_size = 300
            total_batches = (len(chunks) + batch_size - 1) // batch_size
            logger.info(f"Processing {len(chunks)} chunks in {total_batches} batches...")

            successful_chunks = 0
            for batch_num in range(0, len(chunks), batch_size):
                batch_chunks = chunks[batch_num:batch_num + batch_size]
                chunk_texts = [chunk["text"] for chunk in batch_chunks]

                try:
                    embeddings = await fast_embedding_service.get_embeddings_batch(chunk_texts)
                except Exception as e:
                    raise ExternalServiceError(
                        f"Embedding generation failed: {str(e)}",
                        "Error generating embeddings."
                    )

                chunk_records = []
                for i, (chunk, embedding) in enumerate(zip(batch_chunks, embeddings)):
                    chunk_records.append({
                        "document_id": document_id,
                        "user_id": user_id,
                        "chunk_text": chunk["text"],
                        "embedding": embedding,
                        "chunk_index": batch_num + i,
                        "metadata": chunk["metadata"]
                    })

                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, supabase_client.insert_chunks, chunk_records)
                    successful_chunks += len(chunk_records)
                    logger.info(f"Inserted batch {batch_num//batch_size + 1}/{total_batches}")
                    if batch_num + batch_size < len(chunks):
                        await asyncio.sleep(0.1)
                except Exception as e:
                    raise ExternalServiceError(
                        f"Database insertion failed: {str(e)}",
                        "Error saving document to database."
                    )

            if successful_chunks < len(chunks):
                raise ExternalServiceError(
                    f"Incomplete processing: {successful_chunks}/{len(chunks)} chunks",
                    "Document processing incomplete."
                )

        except Exception as e:
            log_error(e, user_id, "_process_chunks_optimized", {"document_id": document_id})
            raise ExternalServiceError(
                f"Unexpected chunk processing error: {str(e)}",
                "Unexpected error during chunk processing."
            )

    async def _check_file_limit(self, user_id: str):
        """Check if user can upload more files"""
        try:
            settings = supabase_client.get_user_settings(user_id)
            active_docs = supabase_client.get_active_documents(user_id)
            if len(active_docs) >= settings["max_files"]:
                raise ValidationError(
                    f"File limit reached ({settings['max_files']} max)",
                    f"You've reached the maximum file limit ({settings['max_files']})."
                )
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            log_error(e, user_id, "_check_file_limit")
            raise ExternalServiceError(
                f"File limit check failed: {str(e)}",
                "Unable to verify file limits."
            )

    async def delete_file(self, user_id: str, file_name: str) -> dict:
        """Delete file and its chunks with error handling"""
        try:
            if not user_id or not file_name:
                raise ValidationError("Invalid user ID or filename", "Invalid identifiers.")

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, supabase_client.soft_delete_document, user_id, file_name)
            await loop.run_in_executor(None, supabase_client.delete_file, f"{user_id}/{file_name}")
            logger.info(f"Deleted: {file_name}")
            return {"status": "success", "message": f"Deleted {file_name}"}

        except Exception as e:
            log_error(e, user_id, "delete_file", {"filename": file_name})
            raise ExternalServiceError(
                f"File deletion failed: {str(e)}",
                "Failed to delete file. Please try again."
            )

    async def get_user_files(self, user_id: str) -> list:
        """Get user's files with error handling"""
        try:
            if not user_id:
                raise ValidationError("Invalid user ID", "Invalid user identifier.")

            docs = supabase_client.get_active_documents(user_id)
            return [doc["file_name"] for doc in docs]
        except Exception as e:
            log_error(e, user_id, "get_user_files")
            raise ExternalServiceError(
                f"Failed to get user files: {str(e)}",
                "Unable to retrieve your files. Please try again."
            )

# Global instance
fast_ingest_service = FastIngestService()
