"""Kafka publisher that writes ML findings to the LLM topic.

Findings are serialised as JSON and published to the topic configured under
``kafka.llm_topic``.  The publisher uses the *at-most-once* delivery model
and logs delivery failures without raising, so that a single publish error
does not interrupt the consume loop.
"""

import json
import logging
from typing import Any, Callable, Dict, Optional

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def _build_producer_config(kafka_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],
        "acks": "all",
        "retries": 3,
    }


class LLMPublisher:
    """Publishes ML findings to the LLM Kafka topic.

    Args:
        kafka_cfg: The ``kafka`` section of the application config.
    """

    def __init__(self, kafka_cfg: Dict[str, Any]) -> None:
        self._cfg = kafka_cfg
        self._topic: str = kafka_cfg["llm_topic"]
        self._producer: Optional[Producer] = None

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "LLMPublisher":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Create the underlying Kafka producer."""
        self._producer = Producer(_build_producer_config(self._cfg))
        logger.info("LLM publisher started (topic='%s').", self._topic)

    def stop(self) -> None:
        """Flush pending messages and close the producer."""
        if self._producer is not None:
            self._producer.flush()
            logger.info("LLM publisher stopped.")

    def publish(self, findings: Dict[str, Any]) -> None:
        """Serialise *findings* as JSON and produce to the LLM topic.

        The message key is set to ``findings["job_id"]`` when available so
        that the downstream LLM consumer can partition by job.

        Args:
            findings: The findings dict returned by
                :meth:`~aiengine.ml_engine.MLEngine.analyse`.
        """
        if self._producer is None:
            raise RuntimeError("Publisher has not been started. Call start() first.")

        message = json.dumps(findings, default=str).encode("utf-8")
        key_raw = findings.get("job_id")
        key = str(key_raw).encode("utf-8") if key_raw is not None else None

        self._producer.produce(
            topic=self._topic,
            key=key,
            value=message,
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)  # Trigger delivery callbacks without blocking

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err:
            logger.error(
                "Failed to deliver message to '%s' [%s]: %s",
                msg.topic(),
                msg.partition(),
                err,
            )
        else:
            logger.debug(
                "Message delivered to '%s' [%s] @ offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
