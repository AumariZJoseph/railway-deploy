from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from routers import query, ingest, auth, files
import os
from logging_config import setup_logging
from services.health_check import test_supabase_connection
from services.error_handler import global_exception_handler, AppError  # Add this import

# Setup structured logging
setup_logging()

# Test connections on startup
test_supabase_connection()

# Create FastAPI app
app = FastAPI(
    title="Brain Bin API",
    description="Production-ready document QA system",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add global exception handler
app.add_exception_handler(Exception, global_exception_handler)
app.add_exception_handler(AppError, global_exception_handler)

# CORS configuration! Uncomment out in prod!
allowed_origins = os.getenv(
    "ALLOWED_ORIGINS", 
    "http://localhost:3000,http://127.0.0.1:3000,192.168.0.61,https://brain-bin-eight.vercel.app"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins, 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(query.router, prefix="/api/v1")
app.include_router(ingest.router, prefix="/api/v1")
app.include_router(auth.router, prefix="/api/v1")
app.include_router(files.router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "version": "2.0.0",
        "database": "postgresql+pgvector"
    }

@app.get("/")
async def root():
    return {"message": "Brain Bin API Server - Production Ready"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        access_log=True,
        log_config=None
    )