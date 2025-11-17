import pyotp
from fastapi import HTTPException, status

def generate_mfa_secret() -> str:
    """Generate a new MFA secret for user"""
    return pyotp.random_base32()

def get_mfa_uri(username: str, secret: str, issuer: str = "SaaSApp") -> str:
    """Return the otpauth URI for authenticator apps"""
    totp = pyotp.TOTP(secret)
    return totp.provisioning_uri(name=username, issuer_name=issuer)

def verify_mfa_token(secret: str, token: str) -> bool:
    """Check if the provided token is valid"""
    totp = pyotp.TOTP(secret)
    return totp.verify(token)

def required_mfa(user, token: str):
    """Raise an error if the user MFA check fails."""
    if user.mfa_secret and not verify_mfa_token(user.mfa_secret, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="Invalid or expired MFA code"
        )