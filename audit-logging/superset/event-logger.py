# Superset action-audit: ships every logged Superset event (dashboard views,
# chart edits, logins, query runs, ...) as a JSON line on Superset's own
# stdout, in addition to Superset's normal DBEventLogger (unchanged -- this
# is additive, the existing "logs" table in Superset's own Postgres keeps
# working exactly as before).
#
# Printing to stdout (rather than POSTing to Fluent Bit's HTTP input, tried
# first) is deliberate: this cluster's fluent-bit build has a confirmed bug
# where its `http` input never hands records to any network output
# (opensearch, es, even loki) -- see ../fluent-bit/values.yaml for the full
# writeup. stdout is already tailed successfully by fluent-bit's existing
# `tail` input, the same path every other pod's logs (and Ranger's audit
# lines) go through. Fluent Bit picks these lines out of that stream by the
# "audit_source":"superset" field below (matched by the
# superset_audit_emitter rewrite_tag rule in ../fluent-bit/values.yaml).
# Named "audit_source", not "_source" -- the latter is a reserved OpenSearch
# metadata field name and documents fail to index if it's reused.
#
# This file's contents get pasted into configOverrides in the superset helm
# values (see ../../README.md "Applying the Trino/Ranger/Superset changes")
# -- the chart writes each configOverrides entry into superset_config.py.
import json
import time

from superset.utils.log import AbstractEventLogger, DBEventLogger


class JsonStdoutEventLogger(AbstractEventLogger):
    """Delegates to DBEventLogger (unchanged behavior), then also prints
    the same event as a single JSON line to stdout for the audit pipeline."""

    def __init__(self):
        self._db_logger = DBEventLogger()

    def log(self, user_id, action, dashboard_id=None, slice_id=None,
            duration_ms=None, referrer=None, extra=None, **kwargs):
        self._db_logger.log(
            user_id, action, dashboard_id=dashboard_id, slice_id=slice_id,
            duration_ms=duration_ms, referrer=referrer, extra=extra, **kwargs,
        )
        payload = {
            "audit_source": "superset",
            "event_time": time.time(),
            "user_id": user_id,
            "action": action,
            "dashboard_id": dashboard_id,
            "slice_id": slice_id,
            "duration_ms": duration_ms,
            "referrer": referrer,
            "extra": extra,
        }
        try:
            print(json.dumps(payload, default=str), flush=True)
        except Exception:
            pass  # audit shipping must never break Superset itself


EVENT_LOGGER = JsonStdoutEventLogger()
