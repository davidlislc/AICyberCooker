"""Payload processor: decodes raw job payloads based on message metadata.

Two payload types are supported:

* **sanitizer** – the payload is base64-encoded binary file content.
  The decoded bytes are returned along with extracted text (when the file
  appears to be text-based) or raw byte statistics.

* **realtime** – the payload is plain ASCII/UTF-8 text (e.g. log lines,
  network events) and is returned as-is together with basic statistics.
"""

import base64
import binascii
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Metadata keys
_SOURCE_FIELD = "source"
_SOURCE_SANITIZER = "sanitizer"
_SOURCE_REALTIME = "realtime"

# Minimum printable-character ratio to classify bytes as "text"
_TEXT_RATIO_THRESHOLD = 0.90


def _is_text_content(data: bytes) -> bool:
    """Return True when *data* consists mostly of printable ASCII characters."""
    if not data:
        return True
    printable = sum(0x20 <= b < 0x7F or b in (0x09, 0x0A, 0x0D) for b in data)
    return (printable / len(data)) >= _TEXT_RATIO_THRESHOLD


def _byte_entropy(data: bytes) -> float:
    """Compute Shannon entropy of *data* (bits per byte, 0–8)."""
    if not data:
        return 0.0
    import math

    freq: Dict[int, int] = {}
    for byte in data:
        freq[byte] = freq.get(byte, 0) + 1
    length = len(data)
    return -sum((c / length) * math.log2(c / length) for c in freq.values())


def _process_sanitizer_payload(payload: str) -> Dict[str, Any]:
    """Decode a base64 payload and extract features from the file content.

    Args:
        payload: Base64-encoded string from a sanitizer-sourced message.

    Returns:
        A dict with keys ``content_bytes``, ``file_size``, ``entropy``,
        ``is_text``, and optionally ``text_content``.

    Raises:
        ValueError: When *payload* is not valid base64.
    """
    try:
        content_bytes = base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"Payload is not valid base64: {exc}") from exc

    is_text = _is_text_content(content_bytes)
    result: Dict[str, Any] = {
        "content_bytes": content_bytes,
        "file_size": len(content_bytes),
        "entropy": _byte_entropy(content_bytes),
        "is_text": is_text,
    }
    if is_text:
        result["text_content"] = content_bytes.decode("utf-8", errors="replace")

    logger.debug(
        "Sanitizer payload decoded: %d bytes, entropy=%.3f, is_text=%s",
        result["file_size"],
        result["entropy"],
        is_text,
    )
    return result


def _process_realtime_payload(payload: str) -> Dict[str, Any]:
    """Process a plain-text realtime payload.

    Args:
        payload: ASCII/UTF-8 text from a realtime-sourced message.

    Returns:
        A dict with keys ``text_content``, ``char_count``, and ``line_count``.
    """
    text = payload if isinstance(payload, str) else str(payload)
    result: Dict[str, Any] = {
        "text_content": text,
        "char_count": len(text),
        "line_count": text.count("\n") + 1 if text else 0,
    }
    logger.debug(
        "Realtime payload processed: %d chars, %d lines",
        result["char_count"],
        result["line_count"],
    )
    return result


def process_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    """Decode and enrich the payload of a raw Kafka job message.

    The function inspects ``job["metadata"]["source"]`` to decide which
    decoding path to use and returns the *job* dict augmented with a
    ``processed_payload`` key.

    Args:
        job: A job dict as returned by :class:`~aiengine.consumer.JobConsumer`.

    Returns:
        The same *job* dict with ``processed_payload`` populated.

    Raises:
        KeyError: When required fields (``metadata``, ``payload``) are absent.
        ValueError: When the source is unknown or the payload is malformed.
    """
    metadata: Dict[str, Any] = job.get("metadata") or {}
    source: str = str(metadata.get(_SOURCE_FIELD, "")).lower()
    payload = job.get("payload")

    if payload is None:
        raise KeyError("Job message is missing the 'payload' field.")

    if source == _SOURCE_SANITIZER:
        processed = _process_sanitizer_payload(str(payload))
    elif source == _SOURCE_REALTIME:
        processed = _process_realtime_payload(str(payload))
    else:
        raise ValueError(
            f"Unknown source '{source}'. Expected '{_SOURCE_SANITIZER}' or '{_SOURCE_REALTIME}'."
        )

    job["processed_payload"] = processed
    return job
