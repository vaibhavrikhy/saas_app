import secrets
from contextlib import asynccontextmanager
from fastapi import Request, FastAPI, Depends, status, HTTPException
from sqlalchemy.orm import Session
import uvicorn
import pyotp

from Response import UserCreateBody, ResponseCreateUser, UserCreateResponse
from db_conection import Base, engine, get_session
from models import User
from operations import add_user
from fastapi.security import OAuth2PasswordRequestForm
from security import create_access_token, verify_token, verify_password, hash_password
from rbac import require_role
from api_key import router as api_key_router
from mfa import generate_mfa_secret, get_mfa_uri, verify_mfa_token
from premium_access import require_premium_user
from user_session import record_login
from github_login import router as github_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield

app = FastAPI(title="SaaS App", lifespan=lifespan)

app.include_router(github_router)
app.include_router(api_key_router)


@app.post(
    "/register/user",
    status_code=status.HTTP_201_CREATED,
    response_model=ResponseCreateUser,
    responses={status.HTTP_409_CONFLICT: {"description": "The user already exists"}},
)
def register(user: UserCreateBody, session: Session = Depends(get_session)) -> dict[str, UserCreateResponse]:
    hashed_pw = hash_password(user.password)

    new_user = add_user(
        session=session,
        username=user.username,
        email=user.email,
        hashed_password=hashed_pw,
    )

    if not new_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username or email already exists",
        )

    user_response = UserCreateResponse(
        username=new_user.username,
        email=new_user.email,
    )

    return {
        "message": "User created successfully",
        "user": user_response,
    }


@app.post("/token")
def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: Session = Depends(get_session),
    access_token=None,
):
    record_login(request, access_token, session)
    user = session.query(User).filter(User.username == form_data.username).first()

    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if user.mfa_enabled:
        mfa_token = form_data.scopes[0] if form_data.scopes else None
        if not mfa_token or not verify_mfa_token(user.mfa_secret, mfa_token):
            raise HTTPException(status_code=401, detail="MFA verification failed")

    access_token = create_access_token({"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}



@app.get("/users/me")
def read_users_me(current_user: str = Depends(verify_token)):
    return {"current_user": current_user}


@app.get("/admin/dashboard")
def admin_dashboard(current_user=Depends(require_role("admin"))):
    return {"message": f"Welcome Admin {current_user.username}"}


@app.get("/premium/access")
def premium_feature(current_user=Depends(require_role("premium"))):
    return {"message": f"Premium access granted to {current_user.username}"}


@app.post("/generate-api-key/{username}")
def generate_api_key(username: str, session: Session = Depends(get_session)):
    user = session.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.api_key = secrets.token_hex(32)
    session.commit()
    return {"username": user.username, "api_key": user.api_key}


@app.post("/enable-mfa/{username}")
def enable_mfa(username: str, session: Session = Depends(get_session)):
    user = session.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    secret = generate_mfa_secret()
    user.mfa_secret = secret
    user.mfa_enabled = True
    session.commit()

    uri = get_mfa_uri(user.username, secret)
    return {"message": "MFA enabled", "secret": secret, "uri": uri}


@app.get("/premium/feature")
def get_premium_feature(current_user=Depends(require_premium_user)):
    return {
        "message": f"Welcome, {current_user.username}! You now have access to premium features 🎉"
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
