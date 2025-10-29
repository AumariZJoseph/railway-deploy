import logging
import sys
from logging.handlers import RotatingFileHandler
import json
import time

class StructuredLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            'app.log', 
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def info(self, message, **kwargs):
        self.logger.info(self._format_message(message, **kwargs))

    def error(self, message, **kwargs):
        self.logger.error(self._format_message(message, **kwargs))

    def warning(self, message, **kwargs):
        self.logger.warning(self._format_message(message, **kwargs))

    def _format_message(self, message, **kwargs):
        log_entry = {
            "message": message,
            "timestamp": time.time(),
            **kwargs
        }
        return json.dumps(log_entry)

# Configure root logger
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            RotatingFileHandler('app.log', maxBytes=10485760, backupCount=5)
        ]
    )