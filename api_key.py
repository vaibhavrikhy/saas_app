from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Header, status
from sqlalchemy.orm import Session
from db_conection import get_session
from models import User
import secrets

router = APIRouter()

def generate_api_key() -> str:
    return secrets.token_hex(32)

async def get_api_key(
        x_api_key: Optional[str] = Header(None),
        session: Session = Depends(get_session),):
    if not x_api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, details = "Missing API key")

    user = session.query(User).filter(User.api_key == x_api_key).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="Invalid API Key"
        )
    return user
@router.get("/secure-data")
async def get_secure_data(user:User = Depends(get_api_key)):
    return {"message": f"Access granted to {user.username}"}
