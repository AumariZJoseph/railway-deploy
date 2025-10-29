import logging
from services.supabase_client import supabase_client

logger = logging.getLogger(__name__)

def test_supabase_connection():
    """Test if Supabase connection is working"""
    try:
        # Simple query to test connection
        result = supabase_client.client.table("documents").select("count", count="exact").limit(1).execute()
        logger.info("Supabase connection test: SUCCESS")
        return True
    except Exception as e:
        logger.error(f"Supabase connection test: FAILED - {str(e)}")
        return False

# Run health check when module is imported
test_supabase_connection()