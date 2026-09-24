"""
Kafka consumer: reads "user's desired Ranger groups" events published by
the OIDC attribute-enrichment layer, and makes Ranger's group membership
match them.

See ../ARCHITECTURE.md for the message schema and the full design
rationale (why full desired-state, not deltas; why this doesn't replace
reconciliation).

Run with:
    python sync_service.py

Configuration is via environment variables -- see the `Config` class below
and k8s/consumer-deployment.yaml for how they're wired up in production.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ranger_client import RangerClient, RangerError, RangerUser, diff_desired_state

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("sync_service")


@dataclass
class Config:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    ranger_url: str
    ranger_user: str
    ranger_password: str
    max_retries: int = 5
    retry_backoff_seconds: float = 2.0

    @classmethod
    def from_env(cls) -> "Config":
        try:
            return cls(
                kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                kafka_topic=os.environ.get("KAFKA_TOPIC", "user-attribute-changed"),
                kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "ranger-group-sync"),
                ranger_url=os.environ["RANGER_URL"],
                ranger_user=os.environ["RANGER_USER"],
                ranger_password=os.environ["RANGER_PASSWORD"],
                max_retries=int(os.environ.get("MAX_RETRIES", "5")),
                retry_backoff_seconds=float(os.environ.get("RETRY_BACKOFF_SECONDS", "2.0")),
            )
        except KeyError as e:
            logger.error("Missing required environment variable: %s", e)
            sys.exit(1)


REQUIRED_FIELDS = ("event_id", "event_time", "username", "ranger_groups")


class ValidationError(Exception):
    pass


def validate_message(msg: dict) -> None:
    missing = [f for f in REQUIRED_FIELDS if f not in msg]
    if missing:
        raise ValidationError(f"missing required field(s): {missing}")
    if not isinstance(msg["ranger_groups"], list):
        raise ValidationError("ranger_groups must be a list")
    if not isinstance(msg["username"], str) or not msg["username"].strip():
        raise ValidationError("username must be a non-empty string")


def process_message(client: RangerClient, msg: dict, last_event_time: dict[str, str]) -> None:
    """Apply one desired-state event to Ranger. Raises on failure so the
    caller can decide whether to retry or dead-letter."""
    validate_message(msg)

    username = msg["username"]
    desired_groups = set(msg["ranger_groups"])
    attrs = msg.get("attributes", {})

    # Ordering guard: drop messages older than the last one we actually
    # applied for this user (see ARCHITECTURE.md "Ordering"). This is an
    # in-memory best-effort check -- fine because being wrong here just
    # means a slightly stale message gets applied and then immediately
    # corrected by the next real message or the reconciliation job, never
    # a permanent error.
    seen = last_event_time.get(username)
    if seen and msg["event_time"] < seen:
        logger.warning(
            "Dropping stale event %s for %s (event_time=%s, already processed %s)",
            msg["event_id"], username, msg["event_time"], seen,
        )
        return

    # Step 1 + 2: ensure the user and every referenced group exist in
    # Ranger BEFORE touching membership -- see RANGER-REST-API-REFERENCE.md
    # gotcha #1. Always do this, even if you're "sure" they already exist.
    client.ensure_users([
        RangerUser(
            username=username,
            first_name=attrs.get("first_name"),
            last_name=attrs.get("last_name"),
            email=attrs.get("email"),
        )
    ])
    client.ensure_groups(sorted(desired_groups))

    # Step 3: diff against Ranger's actual current state and push only
    # the delta -- Ranger is the source of truth for "current", the
    # message is the source of truth for "desired".
    current_groups = client.get_user_current_groups(username)
    diffs = diff_desired_state(current_groups, desired_groups, username)
    if diffs:
        client.sync_membership(diffs)
        logger.info(
            "Synced %s: current=%s desired=%s changes=%s",
            username, sorted(current_groups), sorted(desired_groups),
            [d.to_payload() for d in diffs],
        )
    else:
        logger.debug("%s already matches desired state %s, nothing to do", username, sorted(desired_groups))

    last_event_time[username] = msg["event_time"]


def run():
    config = Config.from_env()
    client = RangerClient(config.ranger_url, config.ranger_user, config.ranger_password)

    consumer = KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_bootstrap_servers.split(","),
        group_id=config.kafka_group_id,
        # Manual commit -- we only advance the offset after Ranger
        # confirms success, so a crash/failure gets the message
        # redelivered rather than silently skipped.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    # In-memory per-username watermark for the staleness check above.
    # Lost on restart -- acceptable, since it's just an optimization on
    # top of idempotent processing, not a correctness requirement.
    last_event_time: dict[str, str] = {}

    logger.info("Listening on topic=%s group_id=%s", config.kafka_topic, config.kafka_group_id)

    for record in consumer:
        msg = record.value
        event_id = msg.get("event_id", "<unknown>")
        attempt = 0
        while True:
            attempt += 1
            try:
                process_message(client, msg, last_event_time)
                consumer.commit()
                break
            except ValidationError as e:
                logger.error("Invalid message %s, sending to DLQ: %s", event_id, e)
                # TODO: publish `msg` + str(e) to your dead-letter topic
                # here. Left as a stub since the DLQ topic/producer setup
                # is environment-specific -- see IMPLEMENTATION-GUIDE.md
                # step 6 for the pattern.
                consumer.commit()  # don't retry a message that will never be valid
                break
            except (RangerError, KafkaError) as e:
                if attempt >= config.max_retries:
                    logger.error(
                        "Giving up on message %s after %d attempts, sending to DLQ: %s",
                        event_id, attempt, e,
                    )
                    # TODO: publish to DLQ, same as above.
                    consumer.commit()
                    break
                backoff = config.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Attempt %d/%d failed for message %s (%s), retrying in %.1fs",
                    attempt, config.max_retries, event_id, e, backoff,
                )
                time.sleep(backoff)


if __name__ == "__main__":
    run()
