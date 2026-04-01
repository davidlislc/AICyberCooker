"""Tests for aiengine.consumer."""

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from aiengine.consumer import JobConsumer, _build_consumer_config

KAFKA_CFG = {
    "bootstrap_servers": "localhost:9092",
    "consumer_group": "test_group",
    "input_topic": "aiengine_in",
    "llm_topic": "llm_in",
    "poll_timeout_ms": 500,
}


# ---------------------------------------------------------------------------
# _build_consumer_config
# ---------------------------------------------------------------------------

def test_build_consumer_config_maps_fields():
    cfg = _build_consumer_config(KAFKA_CFG)
    assert cfg["bootstrap.servers"] == "localhost:9092"
    assert cfg["group.id"] == "test_group"
    assert cfg["enable.auto.commit"] is False


# ---------------------------------------------------------------------------
# JobConsumer.poll
# ---------------------------------------------------------------------------

class _MockMessage:
    """Minimal confluent_kafka.Message stand-in."""

    def __init__(self, value: bytes = b"", error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error

    def partition(self):
        return 0


def _make_consumer(mock_consumer_cls=None):
    consumer = JobConsumer(KAFKA_CFG)
    if mock_consumer_cls is not None:
        consumer._consumer = mock_consumer_cls
    return consumer


def test_poll_returns_none_on_timeout():
    mock_c = MagicMock()
    mock_c.poll.return_value = None
    c = _make_consumer(mock_c)
    assert c.poll() is None


def test_poll_returns_parsed_job():
    payload = json.dumps({"metadata": {"source": "realtime"}, "payload": "hello"}).encode()
    msg = _MockMessage(value=payload)
    mock_c = MagicMock()
    mock_c.poll.return_value = msg
    c = _make_consumer(mock_c)
    job = c.poll()
    assert job is not None
    assert job["metadata"]["source"] == "realtime"
    assert "_kafka_message" in job


def test_poll_raises_on_bad_json():
    msg = _MockMessage(value=b"not-json")
    mock_c = MagicMock()
    mock_c.poll.return_value = msg
    c = _make_consumer(mock_c)
    with pytest.raises(ValueError, match="Invalid message payload"):
        c.poll()


def test_poll_skips_empty_payload():
    msg = _MockMessage(value=None)
    mock_c = MagicMock()
    mock_c.poll.return_value = msg
    c = _make_consumer(mock_c)
    result = c.poll()
    assert result is None


def test_poll_raises_kafka_exception_on_error():
    err_mock = MagicMock()
    err_mock.code.return_value = KafkaError.UNKNOWN  # some real error code
    msg = _MockMessage(error=err_mock)
    mock_c = MagicMock()
    mock_c.poll.return_value = msg
    c = _make_consumer(mock_c)
    with pytest.raises(KafkaException):
        c.poll()


def test_poll_returns_none_on_partition_eof():
    err_mock = MagicMock()
    err_mock.code.return_value = KafkaError._PARTITION_EOF
    msg = _MockMessage(error=err_mock)
    mock_c = MagicMock()
    mock_c.poll.return_value = msg
    c = _make_consumer(mock_c)
    result = c.poll()
    assert result is None


# ---------------------------------------------------------------------------
# JobConsumer.commit
# ---------------------------------------------------------------------------

def test_commit_calls_underlying_commit():
    mock_c = MagicMock()
    c = _make_consumer(mock_c)
    msg = _MockMessage(value=b"{}")
    job = {"_kafka_message": msg}
    c.commit(job)
    mock_c.commit.assert_called_once_with(message=msg, asynchronous=False)
    assert "_kafka_message" not in job


# ---------------------------------------------------------------------------
# JobConsumer lifecycle
# ---------------------------------------------------------------------------

def test_start_raises_without_bootstrap():
    bad_cfg = dict(KAFKA_CFG, bootstrap_servers="")
    consumer = JobConsumer(bad_cfg)
    with patch("aiengine.consumer.Consumer") as MockConsumer:
        MockConsumer.side_effect = Exception("Cannot connect")
        with pytest.raises(Exception, match="Cannot connect"):
            consumer.start()


def test_poll_raises_if_not_started():
    consumer = JobConsumer(KAFKA_CFG)
    with pytest.raises(RuntimeError, match="not been started"):
        consumer.poll()
