from sqlalchemy import Column, Integer, String, Boolean, DateTime
from db_conection import Base

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)

    username = Column(String, unique=True, index=True, nullable=False)

    email = Column(String, unique=True, index=True, nullable=False)

    hashed_password = Column(String, nullable=False)

    role = Column(String, default="user", nullable=False)

    api_key = Column(String, unique=True, nullable=True)

    mfa_secret = Column(String, nullable=True)
    mfa_enabled = Column(Boolean, default=False)

    last_login = Column(DateTime, nullable=True)
    last_ip = Column(String, nullable=True)





