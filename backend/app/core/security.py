"""Authentication and request identity.

Phase 1 uses API keys because the citizen app and partner ingestion feeds are
machine clients. Phase 4 layers operator OIDC/JWT on top for the NDMA console;
:class:`Principal` is the seam that will absorb that change — handlers depend
on the principal, never on the key itself.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from enum import StrEnum

from app.core.config import Settings, get_settings
from app.core.errors import AuthenticationError, AuthorizationError


class Scope(StrEnum):
    """Coarse capabilities attached to a credential."""

    INGEST = "ingest"      # submit incident reports
    READ = "read"          # read public/aggregated views
    VERIFY = "verify"      # human-in-the-loop verification (Phase 4)
    ADMIN = "admin"        # full operator control


@dataclass(frozen=True, slots=True)
class Principal:
    """Authenticated caller."""

    subject: str
    scopes: frozenset[Scope]
    key_fingerprint: str
    is_anonymous: bool = False
    attributes: dict[str, str] = field(default_factory=dict)

    def has(self, scope: Scope) -> bool:
        return Scope.ADMIN in self.scopes or scope in self.scopes

    def require(self, scope: Scope) -> None:
        if not self.has(scope):
            raise AuthorizationError(
                f"Scope {scope.value!r} required",
                subject=self.subject,
                granted=sorted(s.value for s in self.scopes),
            )


ANONYMOUS = Principal(
    subject="anonymous",
    scopes=frozenset({Scope.READ}),
    key_fingerprint="",
    is_anonymous=True,
)


def fingerprint_key(api_key: str) -> str:
    """Short, non-reversible key identifier safe to write into logs.

    Raw keys must never reach the log pipeline; the fingerprint still lets an
    operator correlate abusive traffic to a single credential.
    """
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:16]


def _matches_any(candidate: str, allowed: frozenset[str]) -> bool:
    """Constant-time membership test, resistant to timing side channels."""
    found = False
    for key in allowed:
        # compare_digest over every entry — no early exit.
        if hmac.compare_digest(candidate, key):
            found = True
    return found


def authenticate(api_key: str | None, settings: Settings | None = None) -> Principal:
    """Resolve an API key into a :class:`Principal`.

    Raises :class:`AuthenticationError` when the key is absent or unknown.
    """
    cfg = settings or get_settings()

    if not api_key or not api_key.strip():
        raise AuthenticationError("API key header is required")

    candidate = api_key.strip()
    fp = fingerprint_key(candidate)

    if _matches_any(candidate, cfg.admin_api_key_set):
        return Principal(
            subject=f"admin:{fp}",
            scopes=frozenset({Scope.ADMIN, Scope.VERIFY, Scope.READ, Scope.INGEST}),
            key_fingerprint=fp,
        )

    if _matches_any(candidate, cfg.ingest_api_key_set):
        return Principal(
            subject=f"ingest:{fp}",
            scopes=frozenset({Scope.INGEST, Scope.READ}),
            key_fingerprint=fp,
        )

    raise AuthenticationError("Unrecognised API key", key_fingerprint=fp)


def pseudonymous_author_id(raw_identifier: str, salt: str = "sih26069") -> str:
    """Derive a stable, non-identifying author id for a citizen reporter.

    Phase 2 credibility scoring needs to recognise *"this handle has filed six
    reports today"* without the platform storing a phone number or device id.
    A keyed digest gives cross-report linkability while keeping the raw
    identifier out of the datastore.
    """
    digest = hmac.new(
        salt.encode("utf-8"), raw_identifier.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"anon-{digest[:20]}"
