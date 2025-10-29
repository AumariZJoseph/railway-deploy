from pydantic import BaseModel

class QueryRequest(BaseModel):
    user_id: str
    question: str

class QueryResponse(BaseModel):
    answer: str
    success: bool
    error_message: str = None