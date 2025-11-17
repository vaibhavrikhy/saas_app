from fastapi import Depends, HTTPException, status
from security import verify_token
from db_conection import get_session
from models import User
from sqlalchemy.orm import Session

def get_current_user_obj(username: str = Depends(verify_token),
                     session: Session = Depends(get_session),):
    user = session.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(Status_code=status.HTTP_404_NOT_FOUND, detail = "Invalid User")
    return user

def require_role(required_role: str):
    def role_checker(current_user: User = Depends(get_current_user_obj)):
        if current_user.role != required_role:
            raise HTTPException(Status_code=status.HTTP_403_FORBIDDEN,
                                detail = f"Access restricted to {required_role} role",)
        return current_user
    return role_checker

