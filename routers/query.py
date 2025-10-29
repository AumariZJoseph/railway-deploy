from services.sanitization_service import sanitization_service
from services.rate_limiter import rate_limiter
from fastapi import APIRouter, HTTPException
from models.query_models import QueryRequest, QueryResponse
from services.query_service import query_service

router = APIRouter()

@router.post("/query", response_model=QueryResponse)
async def query_documents(request: QueryRequest):
    """Query a user's knowledge base with input sanitization"""
    # ✅ Check rate limit for queries
    is_limited, message = rate_limiter.is_rate_limited(request.user_id, "query")
    if is_limited:
        raise HTTPException(status_code=429, detail=message)  # 429 = Too Many Requests

    try:
        # === ADD SANITIZATION HERE ===
        sanitized_user_id = sanitization_service.sanitize_user_id(request.user_id)
        sanitized_question = sanitization_service.sanitize_text(request.question)
        
        # ✅ Use sanitized inputs
        result = await query_service.query_documents(sanitized_user_id, sanitized_question)
        
        if not result["success"]:
            raise HTTPException(
                status_code=404,
                detail=result.get("error", "Error processing query")
            )
        
        return QueryResponse(
            answer=result["answer"],
            success=True
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        )

@router.post("/query/clear-context/{user_id}")
async def clear_conversation_context(user_id: str):
    """Clear conversation context for a user"""
    query_service.clear_conversation_context(user_id)
    return {"status": "success", "message": "Conversation context cleared"}
