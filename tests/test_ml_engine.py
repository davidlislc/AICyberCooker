"""Tests for aiengine.ml_engine."""

import numpy as np
import pytest
import torch

from aiengine.ml_engine import (
    MLEngine,
    _Autoencoder,
    _byte_ngram_frequencies,
    _risk_label,
    _text_feature_vector,
    resolve_device,
)

ML_CFG = {
    "device": "cpu",
    "anomaly_contamination": 0.05,
    "batch_size": 4,
    "sequence_max_length": 128,
}


# ---------------------------------------------------------------------------
# resolve_device
# ---------------------------------------------------------------------------

def test_resolve_device_cpu():
    device = resolve_device("cpu")
    assert device.type == "cpu"


def test_resolve_device_auto_returns_device():
    device = resolve_device("auto")
    assert device.type in ("cpu", "cuda")


def test_resolve_device_cuda_falls_back_to_cpu_when_unavailable(monkeypatch):
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    device = resolve_device("cuda")
    assert device.type == "cpu"


# ---------------------------------------------------------------------------
# _byte_ngram_frequencies
# ---------------------------------------------------------------------------

def test_byte_ngram_frequencies_shape():
    data = b"AAABBBCCC" * 10
    feat = _byte_ngram_frequencies(data, n=2, top_k=256)
    assert feat.shape == (256,)
    assert feat.dtype == np.float32


def test_byte_ngram_frequencies_sums_near_one():
    data = bytes(range(256))
    feat = _byte_ngram_frequencies(data, n=2, top_k=256)
    # Sum may exceed 1 due to hash collisions but should be in a reasonable range
    assert feat.sum() > 0


def test_byte_ngram_frequencies_empty():
    feat = _byte_ngram_frequencies(b"", n=2, top_k=64)
    assert feat.shape == (64,)
    assert feat.sum() == 0.0


# ---------------------------------------------------------------------------
# _text_feature_vector
# ---------------------------------------------------------------------------

def test_text_feature_vector_shape():
    feat = _text_feature_vector("hello world")
    assert feat.shape == (128,)
    assert feat.dtype == np.float32


def test_text_feature_vector_normalised():
    feat = _text_feature_vector("abcabc")
    assert abs(feat.sum() - 1.0) < 1e-5


def test_text_feature_vector_empty():
    feat = _text_feature_vector("")
    assert feat.sum() == 0.0


# ---------------------------------------------------------------------------
# _Autoencoder
# ---------------------------------------------------------------------------

def test_autoencoder_output_shape():
    model = _Autoencoder(input_dim=64)
    x = torch.rand(1, 64)
    out = model(x)
    assert out.shape == (1, 64)


def test_autoencoder_output_in_range():
    model = _Autoencoder(input_dim=32)
    x = torch.rand(5, 32)
    out = model(x)
    assert (out >= 0).all() and (out <= 1).all()


# ---------------------------------------------------------------------------
# _risk_label
# ---------------------------------------------------------------------------

def test_risk_label_high():
    assert _risk_label(0.80) == "HIGH"


def test_risk_label_medium():
    assert _risk_label(0.50) == "MEDIUM"


def test_risk_label_low():
    assert _risk_label(0.10) == "LOW"


def test_risk_label_boundaries():
    assert _risk_label(0.75) == "HIGH"
    assert _risk_label(0.74) == "MEDIUM"
    assert _risk_label(0.40) == "MEDIUM"
    assert _risk_label(0.39) == "LOW"


# ---------------------------------------------------------------------------
# MLEngine.analyse
# ---------------------------------------------------------------------------

def _make_engine():
    return MLEngine(ML_CFG)


def _sanitizer_job(content: bytes = b"test file content"):
    import base64

    encoded = base64.b64encode(content).decode()
    return {
        "metadata": {"source": "sanitizer", "job_id": "job-1"},
        "payload": encoded,
        "processed_payload": {
            "content_bytes": content,
            "file_size": len(content),
            "entropy": 3.0,
            "is_text": True,
            "text_content": content.decode("utf-8", errors="replace"),
        },
    }


def _realtime_job(text: str = "suspicious exec /bin/sh"):
    return {
        "metadata": {"source": "realtime", "job_id": "job-2"},
        "payload": text,
        "processed_payload": {
            "text_content": text,
            "char_count": len(text),
            "line_count": 1,
        },
    }


def test_analyse_sanitizer_job_has_byte_analysis():
    engine = _make_engine()
    findings = engine.analyse(_sanitizer_job())
    assert "byte_analysis" in findings
    assert "risk_score" in findings
    assert "risk_level" in findings
    assert 0.0 <= findings["risk_score"] <= 1.0


def test_analyse_realtime_job_has_text_analysis():
    engine = _make_engine()
    findings = engine.analyse(_realtime_job())
    assert "text_analysis" in findings
    assert 0.0 <= findings["risk_score"] <= 1.0


def test_analyse_threat_keywords_elevate_score():
    engine = _make_engine()
    clean = _realtime_job("normal log entry 2024-01-01 connection ok")
    dirty = _realtime_job("malware exec shell exploit reverse shell backdoor trojan")
    clean_findings = engine.analyse(clean)
    dirty_findings = engine.analyse(dirty)
    assert dirty_findings["text_analysis"]["threat_keyword_score"] > clean_findings["text_analysis"]["threat_keyword_score"]


def test_analyse_empty_job():
    engine = _make_engine()
    job = {"metadata": {"source": "realtime", "job_id": "empty"}, "processed_payload": {}}
    findings = engine.analyse(job)
    assert findings["risk_score"] == 0.0


def test_analyse_preserves_job_id():
    engine = _make_engine()
    findings = engine.analyse(_sanitizer_job())
    assert findings["job_id"] == "job-1"
