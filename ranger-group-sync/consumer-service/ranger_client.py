"""
Thin wrapper around the Ranger REST endpoints this system uses.

Every endpoint called here is documented in ../RANGER-REST-API-REFERENCE.md
-- read that first if anything here is surprising, especially the note
about call order (ensure_users/ensure_groups before sync_membership) and
why sync_membership silently no-ops if you skip that.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)


class RangerError(Exception):
    """Raised when Ranger returns a non-2xx response."""


@dataclass
class RangerUser:
    username: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None

    def to_vxuser(self) -> dict:
        return {
            "name": self.username,
            # Ranger silently drops any user with a blank firstName --
            # see RANGER-REST-API-REFERENCE.md #1 gotcha section. Default
            # to the username so this never happens.
            "firstName": self.first_name or self.username,
            "lastName": self.last_name or "",
            "emailAddress": self.email or "",
            "userSource": 1,  # USER_EXTERNAL
            "isVisible": 1,
            "status": 1,  # enabled
            "userRoleList": ["ROLE_USER"],
        }


@dataclass
class MembershipDiff:
    group_name: str
    add_users: set[str] = field(default_factory=set)
    del_users: set[str] = field(default_factory=set)

    def is_empty(self) -> bool:
        return not self.add_users and not self.del_users

    def to_payload(self) -> dict:
        return {
            "groupName": self.group_name,
            "addUsers": sorted(self.add_users),
            "delUsers": sorted(self.del_users),
        }


class RangerClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"Content-Type": "application/json"})

    # -- internal -----------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/service{path}"
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if not resp.ok:
            raise RangerError(f"{method} {path} -> HTTP {resp.status_code}: {resp.text[:500]}")
        return resp

    # -- write path (used by both sync_service.py and reconcile.py) --

    def ensure_users(self, users: list[RangerUser]) -> int:
        """POST /xusers/ugsync/users -- create-or-update, idempotent."""
        if not users:
            return 0
        body = {"vXUsers": [u.to_vxuser() for u in users]}
        resp = self._request("POST", "/xusers/ugsync/users", json=body)
        count = int(resp.text or "0")
        logger.info("ensure_users: %d user(s) created/updated", count)
        return count

    def ensure_groups(self, group_names: list[str], description: str = "Synced from OIDC attribute enrichment") -> int:
        """POST /xusers/ugsync/groups -- create-or-update, idempotent."""
        if not group_names:
            return 0
        body = {
            "vXGroups": [
                {
                    "name": name,
                    "description": description,
                    "groupSource": 1,  # GROUP_EXTERNAL
                    "isVisible": 1,
                }
                for name in group_names
            ]
        }
        resp = self._request("POST", "/xusers/ugsync/groups", json=body)
        count = int(resp.text or "0")
        logger.info("ensure_groups: %d group(s) created/updated", count)
        return count

    def sync_membership(self, diffs: list[MembershipDiff]) -> int:
        """POST /xusers/ugsync/groupusers -- explicit add/del per group.

        Callers MUST have already called ensure_users/ensure_groups for
        every user/group referenced here in this same sync pass -- this
        endpoint silently ignores unknown usernames/groups (see
        RANGER-REST-API-REFERENCE.md).
        """
        payload = [d.to_payload() for d in diffs if not d.is_empty()]
        if not payload:
            logger.debug("sync_membership: nothing to do")
            return 0
        resp = self._request("POST", "/xusers/ugsync/groupusers", json=payload)
        count = int(resp.text or "0")
        logger.info("sync_membership: %d group(s) updated", count)
        return count

    # -- read path (used for diffing / reconciliation) ----------------

    def get_user_id(self, username: str) -> int | None:
        resp = self.session.get(
            f"{self.base_url}/service/xusers/users/userName/{username}",
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            return None
        if not resp.ok:
            raise RangerError(f"GET user {username} -> HTTP {resp.status_code}: {resp.text[:500]}")
        return resp.json().get("id")

    def get_user_current_groups(self, username: str) -> set[str]:
        """Returns the set of Ranger group names `username` currently
        belongs to. Empty set if the user doesn't exist yet in Ranger."""
        user_id = self.get_user_id(username)
        if user_id is None:
            return set()
        resp = self._request("GET", f"/xusers/{user_id}/groups")
        groups = resp.json().get("vXGroups", [])
        return {g["name"] for g in groups}

    def get_group_members(self, group_name: str) -> set[str]:
        """Returns the set of usernames currently in `group_name`.
        Empty set if the group doesn't exist or has no members."""
        resp = self.session.get(
            f"{self.base_url}/service/xusers/groupusers/groupName/{group_name}",
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            return set()
        if not resp.ok:
            raise RangerError(f"GET group members {group_name} -> HTTP {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        # VXGroupUserInfo: {"xuserInfo": [...]} in some versions, or a
        # plain list of usernames in others -- this cluster's Ranger
        # 2.8.0 returns {"xuserInfo": [{"name": "..."}]}. Handle both
        # defensively; verify against your own version before relying on
        # this in production (see the version-checking note at the top
        # of RANGER-REST-API-REFERENCE.md).
        members = data.get("xuserInfo", data if isinstance(data, list) else [])
        return {m["name"] if isinstance(m, dict) else m for m in members}


def diff_desired_state(current_groups: set[str], desired_groups: set[str], username: str) -> list[MembershipDiff]:
    """Given what a user currently belongs to (from Ranger) and what they
    should belong to (from an OIDC event or the IdP's authoritative state),
    return the per-group add/del entries needed to reconcile the two.
    """
    to_add = desired_groups - current_groups
    to_remove = current_groups - desired_groups

    diffs: dict[str, MembershipDiff] = {}
    for group in to_add:
        diffs[group] = MembershipDiff(group_name=group, add_users={username})
    for group in to_remove:
        diffs[group] = MembershipDiff(group_name=group, del_users={username})
    return list(diffs.values())
