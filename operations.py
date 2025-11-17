from passlib.context import CryptContext
from sqlalchemy.orm import Session
from sqlalchemy.util import deprecated

from models import User

pwd_context = CryptContext(schemes=["bcrypt"])

def add_user(session : Session,username:str,password:str,email:str):
    hashed_password = pwd_context.hash(password)
    new_user = User(username=username,email=email,hashed_password=hashed_password)

    session.add(new_user)
    session.commit()
    session.refresh(new_user)
    return new_user


def get_user(session:Session,username_or_email:str):
    return session.query(User).filter((User.username == username_or_email) | (User.email == username_or_email)).first()

