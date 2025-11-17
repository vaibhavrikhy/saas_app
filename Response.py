# we can create a separate module, responses.py, to keep the response classes used for different endpoints.
#creat a model
from typing import Annotated

from pydantic import BaseModel, EmailStr, Field


class UserCreateResponse(BaseModel):
    username: str
    email: EmailStr


class ResponseCreateUser(BaseModel):
    message: Annotated[
        str, Field(default="user created")
    ]
    user: UserCreateResponse
class UserCreateBody(BaseModel):
    username: str
    email: EmailStr
    password: str