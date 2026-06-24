import os
import sys

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()
DASHBOARD_USER: str | None = os.getenv("DASHBOARD_USERNAME")
DASHBOARD_PASS: str | None = os.getenv("DASHBOARD_PASSWORD")

if not DASHBOARD_USER or not DASHBOARD_PASS:
    print("FATAL: DASHBOARD_USERNAME and DASHBOARD_PASSWORD must be set in .env", file=sys.stderr)
    sys.exit(1)


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    if credentials.username != DASHBOARD_USER or credentials.password != DASHBOARD_PASS:
        raise HTTPException(status_code=401)
