# routers/tasks.py - New router for task status
from fastapi import APIRouter, HTTPException
from services.task_queue import background_queue

router = APIRouter()

@router.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of a background task"""
    status = background_queue.get_task_status(task_id)
    
    if status["status"] == "not_found":
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "task_id": task_id,
        "status": status["status"],
        "result": status.get("result"),
        "error": status.get("error")
    }