from datetime import datetime, timedelta
from typing import Optional, Any
from jose import jwt # type: ignore
from pydantic import BaseModel

from core.config import get_settings

settings = get_settings()

class TokenData(BaseModel):
    user_id: Optional[str] = None
    role: Optional[str] = "user" # 'user', 'pro', 'enterprise'

def create_access_token(subject: str | Any, expires_delta: timedelta = None) -> str:
    """ Securely generates JWT tokens for logging in users """
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode = {"exp": expire, "sub": str(subject)}
    
    # Sign with strong SECRET_KEY and HS256 Algorithm
    encoded_jwt = jwt.encode(
        to_encode, 
        settings.SECRET_KEY, 
        algorithm=settings.ALGORITHM
    )
    return encoded_jwt

# Further password hashing functions (bcrypt) go here

async def get_current_user() -> TokenData:
    """
    Mocked Dependency for Authentication.
    In Production, this will decode the Bearer Token from HTTP headers, 
    verify its validity, and return the database user instance/TokenData.
    """
    # Simulating a user extracted from JWT
    return TokenData(user_id="user_live_999", role="user")
