from datetime import datetime
from fastapi import Depends, Request
from sqlalchemy.orm import Session
from db_conection import get_session
from models import User
from security import verify_token

def record_login(
    request: Request,
    token: dict = Depends(verify_token),
    session: Session = Depends(get_session)
):
    """Record each login with timestamp and IP address."""
    username = token["sub"]
    user = session.query(User).filter(User.username == username).first()
    if user:
        user.last_login = datetime.utcnow()
        user.last_ip = request.client.host
        session.commit()
    return user
