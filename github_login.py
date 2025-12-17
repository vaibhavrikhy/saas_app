
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from sqlalchemy.orm import Session
from db_conection import get_session
from models import User
import os

router = APIRouter()
oauth = OAuth()

oauth.register(
    name="github",
    client_id=os.getenv("GITHUB_CLIENT_ID"),
    client_secret=os.getenv("GITHUB_CLIENT_SECRET"),
    access_token_url="https://github.com/login/oauth/access_token",
    authorize_url="https://github.com/login/oauth/authorize",
    api_base_url="https://api.github.com/",
    client_kwargs={"scope": "user:email"},
)

@router.get("/login/github")
async def login_with_github(request: Request):
    redirect_uri = request.url_for("github_auth_callback")
    return await oauth.github.authorize_redirect(request, redirect_uri)

@router.get("/auth/github/callback", name="github_auth_callback")
async def github_auth_callback(
    request: Request,
    session: Session = Depends(get_session)
):
    token = await oauth.github.authorize_access_token(request)
    resp = await oauth.github.get("user", token=token)
    profile = resp.json()

    if not profile or "login" not in profile:
        raise HTTPException(status_code=400, detail="GitHub login failed")

    username = profile["login"]
    email = profile.get("email") or f"{username}@github.com"

    user = session.query(User).filter(User.email == email).first()
    if not user:
        user = User(username=username, email=email, hashed_password="", role="user")
        session.add(user)
        session.commit()

    return RedirectResponse(url=f"/welcome?user={username}")
