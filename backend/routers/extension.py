from typing import Annotated

import dns.resolver
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import AfterValidator, BaseModel, EmailStr

from routers.auth import require_auth
from utils.email import (
    send_gambling_alert,
    send_partner_password,
    send_reset_password,
    send_tamper_alert,
)
from utils.extensions import (
    delete_heartbeats as ext_delete_heartbeats,
)
from utils.extensions import (
    delete_partner,
    get_all_heartbeat_status,
    get_partner,
    log_tamper,
    record_heartbeat,
    restore_partner,
    setup_partner,
)
from utils.extensions import (
    get_status as ext_get_status,
)
from utils.logger import log as log_msg
from utils.settings import get as settings_get

router = APIRouter(tags=["extension"])


def check_email_mx(v: str) -> str:
    domain = v.split("@")[1]
    try:
        dns.resolver.resolve(domain, "MX", lifetime=5)
    except dns.resolver.LifetimeTimeout:
        raise ValueError(f"DNS timeout checking domain {domain}")
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
        raise ValueError(f"Domain {domain} does not accept email (no MX record)")
    return v


class ExtensionSetupBody(BaseModel):
    extension_id: str
    partner_email: Annotated[EmailStr, AfterValidator(check_email_mx)]


@router.post("/extension/setup")
def extension_setup(body: ExtensionSetupBody) -> dict:
    result = setup_partner(body.extension_id, body.partner_email)
    if settings_get().get("auto_heartbeat_on_setup", True):
        record_heartbeat(body.extension_id)
        log_msg(
            "HEARTBEAT",
            f"initial heartbeat recorded for {body.extension_id} (via partner setup)",
        )
    else:
        log_msg(
            "HEARTBEAT",
            f"initial heartbeat SKIPPED for {body.extension_id} (auto_heartbeat_on_setup=false)",  # noqa: E501
        )
    log_msg(
        "PARTNER", f"partner set up for {body.extension_id} -> {body.partner_email}"
    )  # noqa: E501
    email_ok = send_partner_password(body.partner_email, result["password"])
    if not email_ok:
        log_msg("PARTNER", f"email FAILED to {body.partner_email}, rolling back")
        delete_partner(body.extension_id)
        raise HTTPException(
            status_code=502,
            detail={"error": "email_failed", "message": "Failed to send partner email"},
        )
    log_msg("PARTNER", f"email sent to {body.partner_email}")
    return {
        "success": True,
        "password_hash": result["password_hash"],
        "password_salt": result["password_salt"],
    }


class ExtensionHeartbeatBody(BaseModel):
    extension_id: str


@router.post("/extension/heartbeat")
def extension_heartbeat(body: ExtensionHeartbeatBody, request: Request) -> dict:
    ip = request.client.host if request.client else None
    record_heartbeat(body.extension_id, ip)
    log_msg("HEARTBEAT", f"from {body.extension_id}" + (f" ({ip})" if ip else ""))
    return {"ok": True}


class ExtensionTamperBody(BaseModel):
    extension_id: str
    event_type: str
    details: str = ""


@router.post("/extension/tamper-alert")
def extension_tamper_alert(body: ExtensionTamperBody) -> dict:
    log_tamper(body.extension_id, body.event_type, body.details)
    log_msg(
        "TAMPER",
        f"{body.event_type} from {body.extension_id}"
        + (f": {body.details}" if body.details else ""),
    )
    partner = get_partner(body.extension_id)
    if partner:
        send_tamper_alert(
            partner["partner_email"],
            body.event_type,
            details=body.details,
        )
    return {"ok": True}


class ExtensionGamblingAlertBody(BaseModel):
    extension_id: str
    url: str
    gambling_score: float = 0.0


@router.post("/extension/gambling-alert")
def extension_gambling_alert(body: ExtensionGamblingAlertBody) -> dict:
    log_msg(
        "GAMBLING",
        f"alert from {body.extension_id}"
        f" | url={body.url} | score={body.gambling_score}",
    )
    partner = get_partner(body.extension_id)
    if partner:
        send_gambling_alert(
            partner["partner_email"],
            body.url,
            gambling_score=body.gambling_score,
        )
    return {"ok": True}


class ExtensionResetBody(BaseModel):
    extension_id: str


@router.post("/extension/reset-password")
def extension_reset_password(body: ExtensionResetBody) -> dict:
    partner = get_partner(body.extension_id)
    if not partner:
        raise HTTPException(status_code=404, detail="Extension not registered")
    old_hash = partner["password_hash"]
    old_salt = partner["password_salt"]
    result = setup_partner(body.extension_id, partner["partner_email"])
    log_msg("PARTNER", f"password reset for {body.extension_id}")
    email_ok = send_reset_password(partner["partner_email"], result["password"])
    if not email_ok:
        log_msg(
            "PARTNER",
            f"email FAILED on reset to {partner['partner_email']}, rolling back",
        )
        restore_partner(body.extension_id, old_hash, old_salt)
        raise HTTPException(status_code=502, detail="Failed to send email")
    log_msg("PARTNER", f"new password emailed to {partner['partner_email']}")
    return {
        "success": True,
        "password_hash": result["password_hash"],
        "password_salt": result["password_salt"],
    }


@router.get("/extension/status")
def extension_status(
    extension_id: str = Query(...),
    _: None = Depends(require_auth),
) -> dict:
    return ext_get_status(extension_id)


@router.get("/extension/heartbeats")
def extension_heartbeats(_: None = Depends(require_auth)) -> dict:
    heartbeats = get_all_heartbeat_status()
    return {"heartbeats": heartbeats}


@router.delete("/extension/heartbeat/{extension_id}")
def extension_heartbeat_delete(
    extension_id: str,
    _: None = Depends(require_auth),
) -> dict:
    ext_delete_heartbeats(extension_id)
    log_msg("HEARTBEAT", f"heartbeats deleted for {extension_id} (admin)")
    return {"success": True}
