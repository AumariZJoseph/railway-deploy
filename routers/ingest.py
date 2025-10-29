import os
import tempfile
import logging
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from services.file_safety_service import file_safety_service
from services.sanitization_service import sanitization_service
from services.rate_limiter import rate_limiter
from services.fast_ingest_service import fast_ingest_service

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/ingest")
async def ingest_document(
    user_id: str = Form(...),
    file: UploadFile = File(...)
):
    """Fast document ingestion with COMPREHENSIVE safety checks"""
    # ✅ Check rate limit for file operations
    is_limited, message = rate_limiter.is_rate_limited(user_id, "file_operations")
    if is_limited:
        raise HTTPException(status_code=429, detail=message)

    try:
        # === SANITIZE INPUTS ===
        sanitized_user_id = sanitization_service.sanitize_user_id(user_id)
        sanitized_filename = sanitization_service.sanitize_filename(file.filename)

        # ✅ Read file content
        content = await file.read()

        # ✅ Create temp file for safety checking
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{sanitized_filename}") as temp_file:
            temp_file.write(content)
            temp_path = temp_file.name

        try:
            # === COMPREHENSIVE SAFETY CHECK FOR ALL FILE TYPES ===
            is_safe, safety_message = file_safety_service.validate_file_safety(
                temp_path, sanitized_filename
            )
            if not is_safe:
                # Log the security attempt
                file_hash = file_safety_service.calculate_file_hash(temp_path)
                logger.warning(
                    f"🚨 Security rejection: {safety_message} | File: {sanitized_filename} | "
                    f"Hash: {file_hash} | User: {sanitized_user_id}"
                )
                raise HTTPException(status_code=400, detail=f"Security check failed: {safety_message}")

            # ✅ Reset file pointer after reading
            await file.seek(0)

            # ✅ Process file with sanitized inputs
            result = await fast_ingest_service.ingest_file(sanitized_user_id, file)

            return {
                "status": "success",
                "message": "File ingested successfully",
                "filename": sanitized_filename,
                "document_id": result.get("document_id"),
                "chunks_processed": result.get("chunks_processed", 0)
            }

        finally:
            # ✅ Cleanup temp file
            try:
                os.unlink(temp_path)
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete temp file {temp_path}: {str(e)}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ingestion error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing file: {str(e)}"
        )



