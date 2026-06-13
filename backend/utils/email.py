from __future__ import annotations

# ruff: noqa: E501
import logging
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

logger = logging.getLogger(__name__)

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Gambling Blocker")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", SMTP_USER)


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    *,
    from_name: str = SMTP_FROM_NAME,
    from_email: str = SMTP_FROM_EMAIL,
) -> bool:
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.warning("SMTP not configured — skipping email to %s", to_email)
        return False

    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr((from_name, from_email))
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        logger.info("Email sent to %s | subject=%s", to_email, subject)
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP auth failed for %s. Check App Password.", SMTP_USER)
    except smtplib.SMTPException as exc:
        logger.error("SMTP error sending to %s: %s", to_email, exc)
    except OSError as exc:
        logger.error("Network error sending to %s: %s", to_email, exc)
    return False


def send_partner_password(
    partner_email: str,
    password: str,
    *,
    user_email: str = "",
) -> bool:
    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
<h2 style="color:#1a73e8;">Gambling Blocker — Accountability Partner</h2>
<p>Hi,</p>
<p><strong>{user_email}</strong> has registered you as their accountability partner for Gambling Blocker.</p>
<p>The extension is now locked. Only you have the password to access its settings:</p>
<table style="background:#f8f9fa;padding:16px;border-radius:8px;margin:16px 0;width:100%;">
<tr><td style="font-weight:bold;padding-right:12px;">Password:</td>
<td style="font-family:monospace;font-size:18px;letter-spacing:2px;">{password}</td></tr>
</table>
<p style="color:#666;font-size:13px;">Keep this password safe. If the user tries to access extension settings without it, you will receive an alert.</p>
<hr style="border:none;border-top:1px solid #eee;margin:16px 0;">
<p style="color:#999;font-size:11px;">Automated message from Gambling Blocker</p>
</body></html>"""
    return send_email(
        to_email=partner_email,
        subject="Your Gambling Blocker Accountability Password",
        html_body=html,
    )


def send_tamper_alert(
    partner_email: str,
    event_type: str,
    *,
    user_email: str = "",
    details: str = "",
) -> bool:
    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
<h2 style="color:#d93025;">⚠️ Extension Tamper Alert</h2>
<p>The Gambling Blocker extension on <strong>{user_email or "your account"}</strong> has detected suspicious activity:</p>
<table style="background:#fce8e6;padding:16px;border-radius:8px;margin:16px 0;width:100%;">
<tr><td style="font-weight:bold;padding-right:12px;">Event:</td><td>{event_type}</td></tr>
{"" if not details else f"<tr><td style='font-weight:bold;padding-right:12px;vertical-align:top;'>Details:</td><td style='white-space:pre-wrap;'>{details}</td></tr>"}
</table>
<p style="color:#666;font-size:13px;">If you did not expect this, the user may be trying to bypass protection.</p>
<hr style="border:none;border-top:1px solid #eee;margin:16px 0;">
<p style="color:#999;font-size:11px;">Automated alert from Gambling Blocker</p>
</body></html>"""
    return send_email(
        to_email=partner_email,
        subject="⚠️ Gambling Blocker — Tamper Alert",
        html_body=html,
    )


def send_heartbeat_stale_alert(
    partner_email: str,
    hours_since_last: int,
    *,
    user_email: str = "",
) -> bool:
    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
<h2 style="color:#e67e22;">⚠️ Extension Heartbeat Lost</h2>
<p>The Gambling Blocker extension on <strong>{user_email or "your account"}</strong> has not reported in for <strong>{hours_since_last} hours</strong>.</p>
<p style="background:#fef3e2;padding:16px;border-radius:8px;margin:16px 0;">
This may mean the extension has been disabled or uninstalled. Protection may no longer be active.
</p>
<hr style="border:none;border-top:1px solid #eee;margin:16px 0;">
<p style="color:#999;font-size:11px;">Automated alert from Gambling Blocker</p>
</body></html>"""
    return send_email(
        to_email=partner_email,
        subject="⚠️ Gambling Blocker — Heartbeat Lost",
        html_body=html,
    )
