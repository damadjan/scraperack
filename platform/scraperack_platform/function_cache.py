import hashlib
import os
import re
from pathlib import Path

DIGEST = re.compile(r"[0-9a-f]{64}")


def enabled():
    value = os.getenv("SCRAPERACK_FUNCTION_CACHING", "true").strip().lower()
    if value not in {"true", "false"}:
        raise RuntimeError("SCRAPERACK_FUNCTION_CACHING must be set to true or false")
    return value == "true"


def path_for(digest):
    if not DIGEST.fullmatch(digest):
        raise ValueError("Invalid function digest")
    root = Path(
        os.getenv(
            "SCRAPERACK_FUNCTION_CACHE",
            "/home/scraperack/.cache/functions",
        )
    )
    root.mkdir(parents=True, exist_ok=True)
    return root / digest


def validate(function, expected_digest):
    if hashlib.sha256(function).hexdigest() != expected_digest:
        raise ValueError("Function digest does not match its contents")
