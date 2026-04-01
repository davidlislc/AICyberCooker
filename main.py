"""AICyberCooker – main entry point.

Wires together the Kafka consumer, payload processor, ML engine, and LLM
publisher into a single processing pipeline::

    aiengine_in (Kafka) --> JobConsumer
                        --> process_payload
                        --> MLEngine.analyse
                        --> LLMPublisher --> llm_in (Kafka)

Run with::

    python main.py [--config /path/to/config.yaml]
"""

import argparse
import logging
import signal
import threading
import types
from typing import Any, Dict, Optional

from aiengine.config import load_config, setup_logging
from aiengine.consumer import JobConsumer
from aiengine.ml_engine import MLEngine
from aiengine.processor import process_payload
from aiengine.publisher import LLMPublisher

logger = logging.getLogger(__name__)


def _build_handler(ml_engine: MLEngine, publisher: LLMPublisher):
    """Return a job handler closure that processes one job end-to-end."""

    def handle(job: Dict[str, Any]) -> None:
        job_id = job.get("metadata", {}).get("job_id", "<unknown>")
        logger.info("Processing job '%s' …", job_id)

        # 1. Decode / enrich the payload based on metadata source
        job = process_payload(job)

        # 2. Run ML/DL analysis
        findings = ml_engine.analyse(job)

        # 3. Publish findings to the LLM topic
        publisher.publish(findings)
        logger.info("Job '%s' findings published (risk=%s).", job_id, findings.get("risk_level"))

    return handle


def run(config_path: str) -> None:
    """Start the AI engine processing pipeline.

    Args:
        config_path: Path to the YAML configuration file.
    """
    cfg = load_config(config_path)
    setup_logging(cfg)

    kafka_cfg = cfg["kafka"]
    ml_cfg = cfg.get("ml", {})

    stop_event = threading.Event()

    def _shutdown_handler(signum: int, _frame: Optional[types.FrameType]) -> None:
        logger.info("Shutdown signal received (%s) – stopping …", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    ml_engine = MLEngine(ml_cfg)

    with LLMPublisher(kafka_cfg) as publisher:
        with JobConsumer(kafka_cfg) as consumer:
            handler = _build_handler(ml_engine, publisher)
            consumer.consume_loop(handler, stop_event=stop_event)

    logger.info("AI engine stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="AICyberCooker AI Engine")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML configuration file (default: config.yaml)",
    )
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
