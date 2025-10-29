import time
import logging
from typing import Dict, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self):
        # Store request counts: {user_id: {endpoint: [(timestamp, count)]}}
        self.user_requests = defaultdict(lambda: defaultdict(list))
        
        # Rate limits: (max_requests, time_window_seconds)
        self.limits = {
            "query": (10, 60),           # 10 queries per minute per user
            "file_operations": (10, 3600),  # 10 file ops per hour per user
            "total": (100, 86400),       # 100 total requests per day per user
            "groq_global": (30, 60)      # Global Groq limit (shared across users per minute)
        }
        
        # Global Groq rate limiting
        self.groq_requests = []

    def is_rate_limited(self, user_id: str, endpoint_type: str) -> Tuple[bool, str]:
        """
        Check if user is rate limited
        Returns: (is_limited, message)
        """
        current_time = time.time()
        
        # Clean old records first
        self._clean_old_records(user_id, current_time)
        
        # Check specific endpoint limit
        endpoint_limit, endpoint_window = self.limits[endpoint_type]
        endpoint_count = self._get_request_count(user_id, endpoint_type, endpoint_window, current_time)
        
        if endpoint_count >= endpoint_limit:
            if endpoint_type == "query":
                return True, "Too many questions! Please wait 1 minute before asking more."
            elif endpoint_type == "file_operations":
                return True, "Too many file operations! Please wait 1 hour before uploading/deleting more files."
        
        # Check total daily limit
        total_limit, total_window = self.limits["total"]
        total_count = self._get_request_count(user_id, "total", total_window, current_time)
        
        if total_count >= total_limit:
            return True, "Daily request limit reached! Please try again tomorrow."
        
        # Check global Groq limit for queries
        if endpoint_type == "query":
            is_groq_limited, groq_message = self._check_groq_global_limit(current_time)
            if is_groq_limited:
                return True, groq_message
        
        # Record this request
        self._record_request(user_id, endpoint_type, current_time)
        self._record_request(user_id, "total", current_time)
        
        return False, ""

    def _check_groq_global_limit(self, current_time: float) -> Tuple[bool, str]:
        """Check global Groq API rate limits"""
        # Remove old requests older than 1 minute
        self.groq_requests = [t for t in self.groq_requests if current_time - t < 60]
        
        groq_limit, groq_window = self.limits["groq_global"]
        if len(self.groq_requests) >= groq_limit:
            wait_time = 60 - (current_time - self.groq_requests[0])
            return True, f"The AI service is busy. Please wait {wait_time:.0f} seconds and try again."
        
        # Record Groq request
        self.groq_requests.append(current_time)
        return False, ""

    def _clean_old_records(self, user_id: str, current_time: float):
        """Remove records older than the longest time window (1 day)"""
        for endpoint_type in list(self.user_requests[user_id].keys()):
            self.user_requests[user_id][endpoint_type] = [
                (ts, count) for ts, count in self.user_requests[user_id][endpoint_type]
                if current_time - ts < 86400  # 24 hours
            ]
            if not self.user_requests[user_id][endpoint_type]:
                del self.user_requests[user_id][endpoint_type]
        
        if not self.user_requests[user_id]:
            del self.user_requests[user_id]

    def _get_request_count(self, user_id: str, endpoint_type: str, window: int, current_time: float) -> int:
        """Get request count for a specific time window"""
        if user_id not in self.user_requests or endpoint_type not in self.user_requests[user_id]:
            return 0
        
        count = 0
        for timestamp, request_count in self.user_requests[user_id][endpoint_type]:
            if current_time - timestamp < window:
                count += request_count
        return count

    def _record_request(self, user_id: str, endpoint_type: str, current_time: float):
        """Record a new request"""
        current_second = int(current_time)
        if self.user_requests[user_id][endpoint_type] and self.user_requests[user_id][endpoint_type][-1][0] == current_second:
            self.user_requests[user_id][endpoint_type][-1] = (
                current_second,
                self.user_requests[user_id][endpoint_type][-1][1] + 1
            )
        else:
            self.user_requests[user_id][endpoint_type].append((current_second, 1))

# Global instance
rate_limiter = RateLimiter()
