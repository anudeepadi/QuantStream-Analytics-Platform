"""
Authentication Service

Handles user authentication, JWT tokens, and session management.
"""

import jwt
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import os

from .database_service import DatabaseService
from .redis_service import RedisService

logger = logging.getLogger(__name__)

class AuthService:
    """Authentication service for user management and JWT tokens"""
    
    def __init__(self):
        self.secret_key = os.getenv("SECRET_KEY", "your-secret-key-here")
        self.algorithm = "HS256"
        self.access_token_expire_minutes = 60
        self.refresh_token_expire_days = 7
        
        # Services will be injected or initialized separately
        self.db_service: Optional[DatabaseService] = None
        self.redis_service: Optional[RedisService] = None
    
    def set_services(self, db_service: DatabaseService, redis_service: RedisService):
        """Set database and Redis services"""
        self.db_service = db_service
        self.redis_service = redis_service
    
    def hash_password(self, password: str) -> str:
        """Hash password with salt"""
        salt = secrets.token_hex(16)
        password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return f"{salt}:{password_hash.hex()}"
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        try:
            salt, password_hash = hashed.split(':')
            return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex() == password_hash
        except Exception:
            return False
    
    def create_access_token(self, data: Dict[str, Any]) -> str:
        """Create JWT access token"""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        })
        
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    def create_refresh_token(self, data: Dict[str, Any]) -> str:
        """Create JWT refresh token"""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        })
        
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    async def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify JWT token and return user data"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check if token is in Redis blacklist
            if self.redis_service:
                is_blacklisted = await self.redis_service.exists(f"blacklist:{token}")
                if is_blacklisted:
                    return None
            
            # Get user data
            username = payload.get("sub")
            if not username:
                return None
            
            # Check Redis cache first
            if self.redis_service:
                cached_user = await self.redis_service.get(f"user:{username}")
                if cached_user:
                    return cached_user
            
            # Fetch from database
            if self.db_service:
                user_data = await self.db_service.get_user(username)
                if user_data:
                    # Cache user data
                    if self.redis_service:
                        await self.redis_service.set(
                            f"user:{username}",
                            user_data,
                            expire=300  # 5 minutes
                        )
                    return user_data
            
            return None
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.InvalidTokenError:
            logger.warning("Invalid token")
            return None
        except Exception as e:
            logger.error(f"Error verifying token: {e}")
            return None
    
    async def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user credentials"""
        try:
            if not self.db_service:
                # Fallback to mock authentication for demo
                return self._mock_authenticate(username, password)
            
            user = await self.db_service.get_user(username)
            if not user:
                return None
            
            if not self.verify_password(password, user["password_hash"]):
                return None
            
            # Update last login
            await self.db_service.update_last_login(user["id"])
            
            return {
                "id": user["id"],
                "username": user["username"],
                "email": user["email"],
                "role": user["role"],
                "full_name": user["full_name"]
            }
            
        except Exception as e:
            logger.error(f"Error authenticating user: {e}")
            return None
    
    def _mock_authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Mock authentication for demo purposes"""
        
        # Demo users
        mock_users = {
            "admin": {
                "password": "admin123",
                "id": 1,
                "username": "admin",
                "email": "admin@quantstream.ai",
                "role": "administrator",
                "full_name": "System Administrator"
            },
            "analyst": {
                "password": "analyst123",
                "id": 2,
                "username": "analyst",
                "email": "analyst@quantstream.ai",
                "role": "analyst",
                "full_name": "Financial Analyst"
            },
            "trader": {
                "password": "trader123",
                "id": 3,
                "username": "trader",
                "email": "trader@quantstream.ai",
                "role": "trader",
                "full_name": "Quantitative Trader"
            },
            "guest": {
                "password": "guest123",
                "id": 4,
                "username": "guest",
                "email": "guest@quantstream.ai",
                "role": "viewer",
                "full_name": "Guest User"
            }
        }
        
        user = mock_users.get(username.lower())
        if user and user["password"] == password:
            return {k: v for k, v in user.items() if k != "password"}
        
        return None
    
    async def create_user_session(self, user_data: Dict[str, Any]) -> Dict[str, str]:
        """Create user session with access and refresh tokens"""
        
        # Create tokens
        token_data = {"sub": user_data["username"], "user_id": user_data["id"]}
        access_token = self.create_access_token(token_data)
        refresh_token = self.create_refresh_token(token_data)
        
        # Store session in Redis
        if self.redis_service:
            session_data = {
                "user_id": user_data["id"],
                "username": user_data["username"],
                "role": user_data["role"],
                "created_at": datetime.utcnow().isoformat()
            }
            
            await self.redis_service.cache_user_session(
                access_token,
                session_data,
                expire=self.access_token_expire_minutes * 60
            )
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }
    
    async def refresh_access_token(self, refresh_token: str) -> Optional[Dict[str, str]]:
        """Refresh access token using refresh token"""
        try:
            payload = jwt.decode(refresh_token, self.secret_key, algorithms=[self.algorithm])
            
            if payload.get("type") != "refresh":
                return None
            
            username = payload.get("sub")
            if not username:
                return None
            
            # Get user data
            user_data = None
            if self.db_service:
                user_data = await self.db_service.get_user(username)
            else:
                user_data = self._mock_authenticate(username, "")  # For demo
                
            if not user_data:
                return None
            
            # Create new access token
            token_data = {"sub": user_data["username"], "user_id": user_data["id"]}
            new_access_token = self.create_access_token(token_data)
            
            return {
                "access_token": new_access_token,
                "token_type": "bearer"
            }
            
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
            return None
    
    async def logout_user(self, token: str):
        """Logout user by blacklisting token"""
        try:
            # Decode token to get expiration
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            exp = payload.get("exp")
            
            if exp and self.redis_service:
                # Add token to blacklist until it expires
                current_time = datetime.utcnow().timestamp()
                ttl = max(0, int(exp - current_time))
                
                if ttl > 0:
                    await self.redis_service.set(f"blacklist:{token}", "1", expire=ttl)
            
            # Remove from session cache
            if self.redis_service:
                await self.redis_service.invalidate_user_session(token)
            
        except Exception as e:
            logger.error(f"Error logging out user: {e}")
    
    async def check_user_permissions(self, user_data: Dict[str, Any], required_role: str) -> bool:
        """Check if user has required permissions"""
        
        # Role hierarchy
        role_hierarchy = {
            "viewer": 1,
            "analyst": 2,
            "trader": 3,
            "administrator": 4
        }
        
        user_role = user_data.get("role", "viewer")
        user_level = role_hierarchy.get(user_role, 0)
        required_level = role_hierarchy.get(required_role, 0)
        
        return user_level >= required_level
    
    async def get_user_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user profile data"""
        try:
            if self.db_service:
                # In production, would fetch full profile from database
                return await self.db_service.get_user_by_id(user_id)
            else:
                # Mock profile for demo
                return {
                    "id": user_id,
                    "username": f"user_{user_id}",
                    "email": f"user_{user_id}@quantstream.ai",
                    "role": "analyst",
                    "full_name": f"User {user_id}",
                    "created_at": "2024-01-01",
                    "last_login": datetime.utcnow().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return None
    
    async def update_user_profile(self, user_id: int, profile_data: Dict[str, Any]) -> bool:
        """Update user profile"""
        try:
            if self.db_service:
                return await self.db_service.update_user_profile(user_id, profile_data)
            else:
                # Mock update for demo
                return True
                
        except Exception as e:
            logger.error(f"Error updating user profile: {e}")
            return False
    
    async def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        """Change user password"""
        try:
            if self.db_service:
                # Get current user
                user = await self.db_service.get_user_by_id(user_id)
                if not user:
                    return False
                
                # Verify old password
                if not self.verify_password(old_password, user["password_hash"]):
                    return False
                
                # Hash new password
                new_password_hash = self.hash_password(new_password)
                
                # Update in database
                return await self.db_service.update_user_password(user_id, new_password_hash)
            else:
                # Mock password change for demo
                return True
                
        except Exception as e:
            logger.error(f"Error changing password: {e}")
            return False