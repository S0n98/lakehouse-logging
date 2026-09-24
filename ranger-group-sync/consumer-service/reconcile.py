"""
Reconciliation job: the safety net under the event-driven sync in
sync_service.py. Run on a schedule (see k8s/reconcile-cronjob.yaml, hourly
by default) as a standalone one-shot process, not a long-running service.

What it does:
  1. Pull the authoritative attribute state for every relevant user
     directly from the OIDC/AD provider (NOT from Kafka -- if a message
     was ever lost, Kafka has no record of it, so this has to go straight
     to the source of truth).
  2. Pull each user's actual current group membership from Ranger.
  3. Diff and correct, using the exact same RangerClient calls the
     event-driven consumer uses.
  4. Log every correction made -- if this is ever consistently non-zero,
     the event-driven path is unreliable and needs investigating (see
     ARCHITECTURE.md "Observability").

The one function you MUST implement for your real environment is
`fetch_desired_state_from_idp()` below -- this repo has no access to your
actual OIDC provider's API, so it's a stub. Everything else is complete
and reusable as-is.
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass

from ranger_client import RangerClient, RangerUser, diff_desired_state

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("reconcile")


@dataclass
class DesiredUserState:
    username: str
    first_name: str | None
    last_name: str | None
    email: str | None
    ranger_groups: set[str]


def fetch_desired_state_from_idp() -> list[DesiredUserState]:
    """
    TODO: implement this against your real OIDC/AD provider.

    This needs to return the CURRENT, authoritative attribute-derived
    group list for every user that should be synced -- i.e. exactly what
    your OIDC attribute-enrichment step would compute right now, not a
    log of past events. Typically this means either:

      - Querying your OIDC provider's own admin/management API (if it
        exposes one) for all users plus their enrichment attributes, or
      - Querying AD directly for the base attributes and re-running the
        same enrichment logic your OIDC layer uses, if that logic is
        available as a library/function you can call here too.

    Whatever the source, the output shape must match DesiredUserState
    above. Example of what a real implementation returns:

        return [
            DesiredUserState(
                username="alice",
                first_name="Alice",
                last_name="Nguyen",
                email="alice@example.com",
                ranger_groups={"SENSITIVE"},
            ),
            ...
        ]
    """
    raise NotImplementedError(
        "Implement fetch_desired_state_from_idp() for your real OIDC/AD "
        "provider -- see the docstring above."
    )


def reconcile(client: RangerClient, desired_states: list[DesiredUserState]) -> int:
    corrections_made = 0

    # Ensure every user and every group referenced anywhere shows up in
    # Ranger before diffing any single one -- same ordering rule as the
    # event-driven path (RANGER-REST-API-REFERENCE.md gotcha #1).
    client.ensure_users([
        RangerUser(
            username=s.username,
            first_name=s.first_name,
            last_name=s.last_name,
            email=s.email,
        )
        for s in desired_states
    ])
    all_groups = sorted({g for s in desired_states for g in s.ranger_groups})
    client.ensure_groups(all_groups)

    for state in desired_states:
        current_groups = client.get_user_current_groups(state.username)
        diffs = diff_desired_state(current_groups, state.ranger_groups, state.username)
        if not diffs:
            continue

        corrections_made += 1
        logger.warning(
            "Drift detected for %s: Ranger had %s, IdP says %s -- correcting: %s",
            state.username, sorted(current_groups), sorted(state.ranger_groups),
            [d.to_payload() for d in diffs],
        )
        client.sync_membership(diffs)

    return corrections_made


def run():
    ranger_url = os.environ["RANGER_URL"]
    ranger_user = os.environ["RANGER_USER"]
    ranger_password = os.environ["RANGER_PASSWORD"]

    client = RangerClient(ranger_url, ranger_user, ranger_password)

    logger.info("Fetching authoritative state from IdP...")
    desired_states = fetch_desired_state_from_idp()
    logger.info("Got %d user(s) from IdP, reconciling against Ranger...", len(desired_states))

    corrections = reconcile(client, desired_states)

    if corrections:
        logger.warning(
            "Reconciliation made %d correction(s) -- if this keeps "
            "happening every run, the event-driven sync_service.py path "
            "is unreliable and needs investigating, not just silently "
            "patched over by this job forever.",
            corrections,
        )
    else:
        logger.info("Reconciliation complete, no drift found.")

    return 0  # non-zero exit is reserved for job FAILURE (see except below), not drift-found


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception:
        logger.exception("Reconciliation job failed")
        sys.exit(1)
