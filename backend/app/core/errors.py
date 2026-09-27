"""Domain exceptions.

The split that matters operationally is :class:`RetryableError` vs
:class:`PermanentError`. A worker retries the first with backoff and routes the
second straight to the dead-letter topic — no point re-parsing a payload that
will never parse.
"""

from __future__ import annotations

from typing import Any


class PlatformError(Exception):
    """Base class for every error this platform raises deliberately."""

    default_message = "Unexpected platform error"
    error_code = "PLATFORM_ERROR"

    def __init__(self, message: str | None = None, **context: Any) -> None:
        self.message = message or self.default_message
        self.context = context
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "error_type": type(self).__name__,
            "message": self.message,
            "context": self.context,
        }


class RetryableError(PlatformError):
    """Transient failure. The caller should back off and try again."""

    default_message = "Transient failure; retry advised"
    error_code = "RETRYABLE"


class PermanentError(PlatformError):
    """Failure that will recur identically. Send it to the dead-letter topic."""

    default_message = "Permanent failure; message is not reprocessable"
    error_code = "PERMANENT"


# --------------------------------------------------------------- transient --
class UpstreamUnavailableError(RetryableError):
    """An external provider (IMD, OpenWeather, Nominatim) is down or timing out."""

    default_message = "Upstream provider unavailable"
    error_code = "UPSTREAM_UNAVAILABLE"


class MessageBusError(RetryableError):
    """Kafka produce/consume failed."""

    default_message = "Message bus operation failed"
    error_code = "MESSAGE_BUS_ERROR"


class RateLimitedUpstreamError(RetryableError):
    """Provider returned HTTP 429."""

    default_message = "Upstream rate limit hit"
    error_code = "UPSTREAM_RATE_LIMITED"

    def __init__(self, message: str | None = None, retry_after_s: float | None = None, **ctx):
        super().__init__(message, retry_after_s=retry_after_s, **ctx)
        self.retry_after_s = retry_after_s


# --------------------------------------------------------------- permanent --
class NormalizationError(PermanentError):
    """A raw payload could not be mapped onto the unified incident schema."""

    default_message = "Payload could not be normalized"
    error_code = "NORMALIZATION_FAILED"


class UnsupportedSourceError(PermanentError):
    """No normalizer is registered for this source type."""

    default_message = "No normalizer registered for source"
    error_code = "UNSUPPORTED_SOURCE"


class InvalidPayloadError(PermanentError):
    """Structurally invalid message — bad JSON, missing envelope fields."""

    default_message = "Malformed payload"
    error_code = "INVALID_PAYLOAD"


# ------------------------------------------------------------ API surface ---
class ApiError(PlatformError):
    """Base for errors that map onto an HTTP status code."""

    status_code = 400
    error_code = "BAD_REQUEST"


class AuthenticationError(ApiError):
    status_code = 401
    error_code = "UNAUTHENTICATED"
    default_message = "Missing or invalid API key"


class AuthorizationError(ApiError):
    status_code = 403
    error_code = "FORBIDDEN"
    default_message = "API key lacks the required scope"


class RateLimitExceededError(ApiError):
    status_code = 429
    error_code = "RATE_LIMITED"
    default_message = "Too many requests"

    def __init__(self, message: str | None = None, retry_after_s: int = 60, **ctx):
        super().__init__(message, retry_after_s=retry_after_s, **ctx)
        self.retry_after_s = retry_after_s


class PayloadTooLargeError(ApiError):
    status_code = 413
    error_code = "PAYLOAD_TOO_LARGE"
    default_message = "Uploaded media exceeds the configured size limit"


class UnsupportedMediaError(ApiError):
    status_code = 415
    error_code = "UNSUPPORTED_MEDIA_TYPE"
    default_message = "Unsupported media type"


class ServiceUnavailableError(ApiError):
    status_code = 503
    error_code = "SERVICE_UNAVAILABLE"
    default_message = "A dependency required to serve this request is unavailable"
