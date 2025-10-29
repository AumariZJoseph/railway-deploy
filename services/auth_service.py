import os
import httpx
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

load_dotenv()

logger = logging.getLogger(__name__)

class AuthService:
    def __init__(self):
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_SERVICE_KEY")
        if self.url and self.key:
            self.client: Client = create_client(self.url, self.key)
            # Configure session settings for token expiration
            self.session_duration = timedelta(hours=24)  # 24-hour expiration
        else:
            self.client = None
            logger.error("Supabase credentials not found. Auth features may not work.")
    
    def login(self, email: str, password: str):
        """Authenticate a user with email and password - with token expiration"""
        try:
            response = self.client.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            
            # Calculate expiration time
            expires_at = datetime.now() + self.session_duration
            
            return {
                "status": "success",
                "data": {
                    "access_token": response.session.access_token,
                    "refresh_token": response.session.refresh_token,
                    "user": {
                        "id": response.user.id,
                        "email": response.user.email,
                        "user_metadata": response.user.user_metadata
                    },
                    "expires_at": expires_at.isoformat(),  # Add expiration time
                    "expires_in_hours": 24  # Make it clear to frontend
                }
            }
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            return {
                "status": "error",
                "message": "Invalid credentials"
            }
    
    def register(self, email: str, password: str):
        """Register a new user with email and password - with token expiration"""
        try:
            response = self.client.auth.sign_up({
                "email": email,
                "password": password,
                "options": {
                    "email_redirect_to": "http://localhost:3000/login"
                }
            })
            
            expires_at = datetime.now() + self.session_duration
            
            # If email confirmations are disabled, session will exist
            if response.session:
                return {
                    "status": "success",
                    "message": "Registration successful",
                    "data": {
                        "access_token": response.session.access_token,
                        "refresh_token": response.session.refresh_token,
                        "user": {
                            "id": response.user.id,
                            "email": response.user.email,
                            "user_metadata": response.user.user_metadata
                        },
                        "expires_at": expires_at.isoformat(),
                        "expires_in_hours": 24
                    }
                }
            else:
                # Email confirmation required
                return {
                    "status": "success",
                    "message": "Registration successful. Please check your email for confirmation.",
                    "data": {
                        "user": {
                            "id": response.user.id,
                            "email": response.user.email,
                        }
                    }
                }
            
        except Exception as e:
            error_msg = str(e)
            if "User already registered" in error_msg:
                return {
                    "status": "error",
                    "message": "User already exists with this email"
                }
            return {
                "status": "error",
                "message": f"Registration failed: {error_msg}"
            }
    
    def logout(self, access_token: str):
        """Log out the user by calling the Supabase Auth API directly"""
        try:
            logout_url = f"{self.url}/auth/v1/logout"
            
            with httpx.Client() as client:
                response = client.post(
                    logout_url,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "apikey": self.key
                    }
                )
                
                if response.status_code in [200, 204]:
                    return {
                        "status": "success",
                        "message": "Logged out successfully"
                    }
                else:
                    return {
                        "status": "error",
                        "message": f"Logout failed with status code: {response.status_code}"
                    }
                    
        except Exception as e:
            return {
                "status": "error",
                "message": f"Logout failed: {str(e)}"
            }
    
    def refresh_token(self, refresh_token: str):
        """Refresh an expired access token"""
        try:
            response = self.client.auth.refresh_session(refresh_token)
            
            if response.session:
                expires_at = datetime.now() + self.session_duration
                return {
                    "status": "success",
                    "data": {
                        "access_token": response.session.access_token,
                        "refresh_token": response.session.refresh_token,
                        "expires_at": expires_at.isoformat(),
                        "expires_in_hours": 24
                    }
                }
            else:
                return {
                    "status": "error",
                    "message": "Token refresh failed"
                }
        except Exception as e:
            logger.error(f"Token refresh error: {str(e)}")
            return {
                "status": "error",
                "message": "Token refresh failed"
            }

# Global instance
auth_service = AuthService()