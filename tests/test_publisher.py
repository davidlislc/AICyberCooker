"""Tests for aiengine.publisher."""

import json
from unittest.mock import MagicMock, call, patch

import pytest

from aiengine.publisher import LLMPublisher, _build_producer_config

KAFKA_CFG = {
    "bootstrap_servers": "localhost:9092",
    "consumer_group": "test_group",
    "input_topic": "aiengine_in",
    "llm_topic": "llm_in",
}


# ---------------------------------------------------------------------------
# _build_producer_config
# ---------------------------------------------------------------------------

def test_build_producer_config_maps_fields():
    cfg = _build_producer_config(KAFKA_CFG)
    assert cfg["bootstrap.servers"] == "localhost:9092"
    assert cfg["acks"] == "all"
    assert cfg["retries"] == 3


# ---------------------------------------------------------------------------
# LLMPublisher.publish
# ---------------------------------------------------------------------------

def _make_publisher():
    pub = LLMPublisher(KAFKA_CFG)
    pub._producer = MagicMock()
    return pub


def test_publish_produces_correct_topic():
    pub = _make_publisher()
    findings = {"job_id": "job-42", "risk_score": 0.3, "risk_level": "LOW"}
    pub.publish(findings)
    call_kwargs = pub._producer.produce.call_args
    assert call_kwargs.kwargs["topic"] == "llm_in"


def test_publish_uses_job_id_as_key():
    pub = _make_publisher()
    findings = {"job_id": "abc-123", "risk_level": "HIGH"}
    pub.publish(findings)
    call_kwargs = pub._producer.produce.call_args
    assert call_kwargs.kwargs["key"] == b"abc-123"


def test_publish_message_is_valid_json():
    pub = _make_publisher()
    findings = {"job_id": "x", "risk_score": 0.9, "tags": ["malware"]}
    pub.publish(findings)
    raw = pub._producer.produce.call_args.kwargs["value"]
    parsed = json.loads(raw)
    assert parsed["job_id"] == "x"
    assert parsed["risk_score"] == 0.9


def test_publish_null_job_id_key():
    pub = _make_publisher()
    findings = {"risk_score": 0.1}  # no job_id
    pub.publish(findings)
    call_kwargs = pub._producer.produce.call_args
    assert call_kwargs.kwargs["key"] is None


def test_publish_raises_if_not_started():
    pub = LLMPublisher(KAFKA_CFG)
    with pytest.raises(RuntimeError, match="not been started"):
        pub.publish({"job_id": "x"})


# ---------------------------------------------------------------------------
# LLMPublisher lifecycle
# ---------------------------------------------------------------------------

def test_start_creates_producer():
    with patch("aiengine.publisher.Producer") as MockProducer:
        pub = LLMPublisher(KAFKA_CFG)
        pub.start()
        MockProducer.assert_called_once()
        assert pub._producer is not None


def test_stop_flushes_producer():
    pub = _make_publisher()
    pub.stop()
    pub._producer.flush.assert_called_once()


def test_context_manager_starts_and_stops():
    with patch("aiengine.publisher.Producer") as MockProducer:
        mock_prod = MagicMock()
        MockProducer.return_value = mock_prod
        with LLMPublisher(KAFKA_CFG) as pub:
            assert pub._producer is mock_prod
        mock_prod.flush.assert_called_once()


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def test_delivery_callback_logs_on_error(caplog):
    import logging

    msg_mock = MagicMock()
    msg_mock.topic.return_value = "llm_in"
    msg_mock.partition.return_value = 0

    with caplog.at_level(logging.ERROR, logger="aiengine.publisher"):
        LLMPublisher._delivery_callback("some error", msg_mock)

    assert any("Failed to deliver" in r.message for r in caplog.records)


def test_delivery_callback_no_log_on_success(caplog):
    import logging

    msg_mock = MagicMock()
    msg_mock.topic.return_value = "llm_in"
    msg_mock.partition.return_value = 0
    msg_mock.offset.return_value = 10

    with caplog.at_level(logging.ERROR, logger="aiengine.publisher"):
        LLMPublisher._delivery_callback(None, msg_mock)

    assert not any(r.levelno >= logging.ERROR for r in caplog.records)
