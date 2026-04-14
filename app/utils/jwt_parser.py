"""
JWT token parsing helpers.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import jwt

from app.utils.time_utils import get_now, parse_unix_timestamp


logger = logging.getLogger(__name__)


class JWTParser:
    """Parse ChatGPT access tokens without relying on host-local time."""

    def decode_token(self, token: str) -> Optional[Dict[str, Any]]:
        try:
            # This helper only performs unsigned payload decoding today.
            # Real signature validation needs parser support for keys/JWKs/algorithms.
            return jwt.decode(
                token,
                options={
                    "verify_signature": False,
                    "verify_exp": False,
                },
            )
        except jwt.InvalidTokenError as exc:
            logger.error(f"JWT token decode failed: {exc}")
            return None
        except Exception as exc:
            logger.error(f"Unexpected JWT decode error: {exc}")
            return None

    def extract_email(self, token: str) -> Optional[str]:
        payload = self.decode_token(token)
        if not payload:
            return None

        try:
            profile = payload.get("https://api.openai.com/profile", {})
            return profile.get("email")
        except Exception as exc:
            logger.error(f"Failed to extract email from JWT: {exc}")
            return None

    def get_expiration_time(self, token: str) -> Optional[datetime]:
        payload = self.decode_token(token)
        if not payload:
            return None

        try:
            return parse_unix_timestamp(payload.get("exp"))
        except Exception as exc:
            logger.error(f"Failed to extract expiration time from JWT: {exc}")
            return None

    def is_token_expired(self, token: str) -> bool:
        exp_time = self.get_expiration_time(token)
        if not exp_time:
            return True
        return get_now() >= exp_time
