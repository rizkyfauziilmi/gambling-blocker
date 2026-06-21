import os

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()
DASHBOARD_USER: str = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASS: str = os.getenv("DASHBOARD_PASSWORD", "admin123")


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    if credentials.username != DASHBOARD_USER or credentials.password != DASHBOARD_PASS:
        raise HTTPException(status_code=401)
