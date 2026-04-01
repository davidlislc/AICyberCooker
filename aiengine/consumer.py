"""Kafka consumer for the ``aiengine_in`` topic.

Reads job messages, decodes them, and yields structured job dicts to the
caller.  Each consumed message is committed only after the caller signals
success, ensuring at-least-once delivery.
"""

import json
import logging
from typing import Any, Callable, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)

# Metadata field that identifies the origin of the message.
_SOURCE_FIELD = "source"
_SOURCE_SANITIZER = "sanitizer"
_SOURCE_REALTIME = "realtime"


def _build_consumer_config(kafka_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Translate the application kafka config into a confluent-kafka config dict."""
    return {
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],
        "group.id": kafka_cfg["consumer_group"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


class JobConsumer:
    """Kafka consumer that reads job messages from ``aiengine_in``.

    Args:
        kafka_cfg: The ``kafka`` section of the application config.
    """

    def __init__(self, kafka_cfg: Dict[str, Any]) -> None:
        self._cfg = kafka_cfg
        self._topic = kafka_cfg["input_topic"]
        self._poll_timeout = kafka_cfg.get("poll_timeout_ms", 1000) / 1000.0
        self._consumer: Optional[Consumer] = None

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "JobConsumer":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Create the Kafka consumer and subscribe to the input topic."""
        self._consumer = Consumer(_build_consumer_config(self._cfg))
        self._consumer.subscribe([self._topic])
        logger.info("Subscribed to Kafka topic '%s'", self._topic)

    def stop(self) -> None:
        """Commit pending offsets and close the consumer."""
        if self._consumer is not None:
            self._consumer.close()
            logger.info("Kafka consumer closed.")

    def poll(self) -> Optional[Dict[str, Any]]:
        """Poll for a single message and return a parsed job dict.

        Returns ``None`` when no message is available within the timeout.

        Raises:
            KafkaException: On unrecoverable Kafka errors.
            ValueError: When the message payload cannot be parsed as JSON.
        """
        if self._consumer is None:
            raise RuntimeError("Consumer has not been started. Call start() first.")

        msg: Optional[Message] = self._consumer.poll(timeout=self._poll_timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Reached end of partition %s", msg.partition())
                return None
            raise KafkaException(msg.error())

        raw = msg.value()
        if raw is None:
            logger.warning("Received message with empty payload – skipping.")
            self._commit(msg)
            return None

        try:
            job = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Failed to decode message: %s", exc)
            self._commit(msg)
            raise ValueError(f"Invalid message payload: {exc}") from exc

        job["_kafka_message"] = msg
        return job

    def commit(self, job: Dict[str, Any]) -> None:
        """Commit the offset for a successfully processed job.

        Args:
            job: The job dict returned by :meth:`poll`.
        """
        msg = job.pop("_kafka_message", None)
        if msg is not None:
            self._commit(msg)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _commit(self, msg: Message) -> None:
        if self._consumer is not None:
            self._consumer.commit(message=msg, asynchronous=False)

    def consume_loop(
        self,
        handler: Callable[[Dict[str, Any]], None],
        *,
        stop_event: Any = None,
    ) -> None:
        """Continuously poll and call *handler* for each job until stopped.

        Args:
            handler: Callable that receives a job dict and processes it.
            stop_event: Optional :class:`threading.Event`-like object; the
                loop terminates when ``stop_event.is_set()`` returns ``True``.
        """
        logger.info("Starting consume loop on topic '%s'.", self._topic)
        while stop_event is None or not stop_event.is_set():
            try:
                job = self.poll()
            except ValueError:
                continue  # already logged; skip bad message

            if job is None:
                continue

            try:
                handler(job)
                self.commit(job)
            except Exception:  # pylint: disable=broad-except
                logger.exception("Error processing job %s", job.get("metadata", {}).get("job_id", "<unknown>"))
