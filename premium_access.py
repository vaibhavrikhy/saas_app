from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from db_conection import get_session
from models import User
from security import verify_token

def require_premium_user(
    token: str = Depends(verify_token),
    session: Session = Depends(get_session)
) -> User:
    """Dependency that allows only premium or admin users."""
    username = token["sub"]
    user = session.query(User).filter(User.username == username).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.role not in ("premium", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Premium subscription required"
        )
    return user
