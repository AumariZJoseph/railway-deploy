from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.supabase_client import supabase_client
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

class WaitlistRequest(BaseModel):
    user_id: str
    email: str

@router.post("/waitlist")
async def join_waitlist(request: WaitlistRequest):
    """Add user to waitlist"""
    try:
        # Check if user already in waitlist
        existing = supabase_client.client.table("waitlist")\
            .select("*")\
            .eq("user_id", request.user_id)\
            .execute()
        
        if existing.data:
            return {
                "status": "success", 
                "message": "You're already on the waitlist!"
            }
        
        # Add to waitlist
        result = supabase_client.client.table("waitlist")\
            .insert({
                "user_id": request.user_id,
                "email": request.email
            })\
            .execute()
            
        return {
            "status": "success",
            "message": "You've been added to the waitlist! We'll notify you when the full version launches."
        }
        
    except Exception as e:
        logger.error(f"Waitlist error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to join waitlist")
