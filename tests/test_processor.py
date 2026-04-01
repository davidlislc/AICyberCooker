"""Tests for aiengine.processor."""

import base64

import pytest

from aiengine.processor import (
    _byte_entropy,
    _is_text_content,
    _process_realtime_payload,
    _process_sanitizer_payload,
    process_payload,
)


# ---------------------------------------------------------------------------
# _is_text_content
# ---------------------------------------------------------------------------

def test_is_text_content_plain_text():
    assert _is_text_content(b"Hello, World!\n") is True


def test_is_text_content_binary():
    assert _is_text_content(bytes(range(256))) is False


def test_is_text_content_empty():
    assert _is_text_content(b"") is True


# ---------------------------------------------------------------------------
# _byte_entropy
# ---------------------------------------------------------------------------

def test_byte_entropy_uniform():
    data = bytes(range(256))
    entropy = _byte_entropy(data)
    assert abs(entropy - 8.0) < 0.01  # uniform → max entropy


def test_byte_entropy_constant():
    data = b"\x00" * 100
    entropy = _byte_entropy(data)
    assert entropy == 0.0


def test_byte_entropy_empty():
    assert _byte_entropy(b"") == 0.0


# ---------------------------------------------------------------------------
# _process_sanitizer_payload
# ---------------------------------------------------------------------------

def test_process_sanitizer_text_payload():
    text = "Hello from sanitizer"
    encoded = base64.b64encode(text.encode()).decode()
    result = _process_sanitizer_payload(encoded)
    assert result["file_size"] == len(text.encode())
    assert result["is_text"] is True
    assert result["text_content"] == text


def test_process_sanitizer_binary_payload():
    data = bytes(range(256))
    encoded = base64.b64encode(data).decode()
    result = _process_sanitizer_payload(encoded)
    assert result["file_size"] == 256
    assert result["is_text"] is False
    assert "text_content" not in result


def test_process_sanitizer_invalid_base64():
    with pytest.raises(ValueError, match="not valid base64"):
        _process_sanitizer_payload("!!!not_base64!!!")


# ---------------------------------------------------------------------------
# _process_realtime_payload
# ---------------------------------------------------------------------------

def test_process_realtime_payload():
    text = "line1\nline2\nline3"
    result = _process_realtime_payload(text)
    assert result["text_content"] == text
    assert result["char_count"] == len(text)
    assert result["line_count"] == 3


def test_process_realtime_payload_single_line():
    result = _process_realtime_payload("no newlines")
    assert result["line_count"] == 1


def test_process_realtime_payload_empty():
    result = _process_realtime_payload("")
    assert result["line_count"] == 0
    assert result["char_count"] == 0


# ---------------------------------------------------------------------------
# process_payload
# ---------------------------------------------------------------------------

def _make_job(source: str, payload: str) -> dict:
    return {
        "metadata": {"source": source, "job_id": "test-job-1"},
        "payload": payload,
    }


def test_process_payload_sanitizer():
    encoded = base64.b64encode(b"file content").decode()
    job = _make_job("sanitizer", encoded)
    result = process_payload(job)
    assert "processed_payload" in result
    assert result["processed_payload"]["file_size"] == len(b"file content")


def test_process_payload_realtime():
    job = _make_job("realtime", "network event log line")
    result = process_payload(job)
    assert "processed_payload" in result
    assert result["processed_payload"]["text_content"] == "network event log line"


def test_process_payload_unknown_source():
    job = _make_job("unknown_source", "data")
    with pytest.raises(ValueError, match="Unknown source"):
        process_payload(job)


def test_process_payload_missing_payload():
    job = {"metadata": {"source": "realtime"}}
    with pytest.raises(KeyError, match="payload"):
        process_payload(job)


def test_process_payload_case_insensitive_source():
    encoded = base64.b64encode(b"data").decode()
    job = _make_job("SANITIZER", encoded)
    result = process_payload(job)
    assert "processed_payload" in result
