import re
import html
import logging
from typing import List

logger = logging.getLogger(__name__)

class SanitizationService:
    def __init__(self):
        # Patterns for potentially dangerous content
        self.suspicious_patterns = [
            r'<script.*?>.*?</script>',  # Script tags
            r'javascript:',              # JavaScript protocol
            r'on\w+\s*=',               # Event handlers (onclick, onload, etc.)
            r'expression\s*\(',          # CSS expressions
            r'vbscript:',                # VBScript
            r'<iframe.*?>.*?</iframe>',  # Iframe tags
            r'<object.*?>.*?</object>',  # Object tags
            r'<embed.*?>.*?</embed>',    # Embed tags
            r'<form.*?>.*?</form>',      # Form tags
            r'<link.*?>.*?</link>',      # Link tags
            r'meta\s+http-equiv',        # Meta refresh
        ]
        
        # Compile patterns for better performance
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE | re.DOTALL) 
                                 for pattern in self.suspicious_patterns]
    
    def sanitize_text(self, text: str, max_length: int = 5000) -> str:
        """
        Sanitize user text input by removing dangerous content
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Step 1: Trim to maximum length
        text = text[:max_length]
        
        # Step 2: HTML escape to prevent XSS
        text = html.escape(text)
        
        # Step 3: Remove suspicious patterns (in case they were double-encoded or tricky)
        for pattern in self.compiled_patterns:
            text = pattern.sub('', text)
        
        # Step 4: Remove excessive whitespace but preserve basic formatting
        text = re.sub(r'\s+', ' ', text).strip()
        
        logger.debug(f"Sanitized text: {text[:100]}...")
        return text
    
    def sanitize_filename(self, filename: str) -> str:
        """
        Sanitize file names to prevent path traversal and other attacks
        """
        if not filename:
            return "unknown"
        
        # Remove path components to prevent directory traversal
        filename = re.sub(r'.*[\\/]', '', filename)
        
        # Remove dangerous characters
        filename = re.sub(r'[<>:"|?*\\/\0]', '', filename)
        
        # Limit length
        filename = filename[:255]
        
        # Ensure it has a safe extension
        safe_extensions = {'.pdf', '.docx', '.doc', '.txt', '.csv', '.xlsx', '.xls'}
        file_extension = '.' + filename.split('.')[-1].lower() if '.' in filename else ''
        
        if file_extension and file_extension not in safe_extensions:
            # If extension is not safe, remove it
            filename = re.sub(r'\.[^.]*$', '', filename)
        
        return filename
    
    def sanitize_user_id(self, user_id: str) -> str:
        """
        Sanitize user IDs to prevent injection attacks
        """
        if not user_id:
            return ""
        
        # Only allow alphanumeric, hyphens, and underscores
        user_id = re.sub(r'[^a-zA-Z0-9\-_]', '', user_id)
        
        # Limit length
        return user_id[:100]

# Global instance
sanitization_service = SanitizationService()