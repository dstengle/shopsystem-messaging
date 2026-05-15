"""Postgres-backed storage for the shop-msg mailbox protocol.

All inter-shop messages (inbox from lead, outbox from BC) are stored in
a single 'messages' table. The `bc` column namespaces each row to a
specific Bounded Context root (its filesystem path, which is unique per
test invocation and per real BC deployment).

Configuration — connection DSN:
  The DSN is read from the environment variable SHOPMSG_DSN. If that
  variable is not set, a hardcoded default is used (suitable for
  development and the BDD test suite). In production, operators set
  SHOPMSG_DSN to point at the real Postgres instance.

  Default DSN: postgresql://postgres:postgres@postgres:5432/shopsystem

Schema (DDL emitted once per connect):
  CREATE TABLE IF NOT EXISTS messages (
    id          BIGSERIAL PRIMARY KEY,
    bc          TEXT NOT NULL,
    work_id     TEXT NOT NULL,
    direction   TEXT NOT NULL CHECK (direction IN ('inbox','outbox')),
    message_type TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bc, work_id, direction, message_type)
  );

Collision handling:
  INSERT ... ON CONFLICT DO NOTHING returns 0 rows affected. The
  callers check this and raise CollisionError, which the CLI surfaces
  as a non-zero exit.

LISTEN/NOTIFY:
  After every inbox INSERT, NOTIFY is fired on the channel
  `inbox_<bc_slug>` (with the bc path slug-encoded) carrying the
  work_id as the payload. This is a fire-and-forget from the storage
  layer's perspective; agents hold a long-lived LISTEN connection
  separately.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row


# ---------------------------------------------------------------------------
# DSN / connection
# ---------------------------------------------------------------------------

_DEFAULT_DSN = "postgresql://postgres:postgres@postgres:5432/shopsystem"


def _get_dsn() -> str:
    """Return the DSN from SHOPMSG_DSN env var or the default."""
    return os.environ.get("SHOPMSG_DSN", _DEFAULT_DSN)


def _connect() -> psycopg.Connection:
    """Open a new connection and ensure the schema exists.

    Raises a clear, descriptive error if the DSN is unreachable so the
    operator knows which endpoint to check.
    """
    dsn = _get_dsn()
    try:
        conn = psycopg.connect(dsn, row_factory=dict_row)
    except psycopg.OperationalError as exc:
        raise RuntimeError(
            f"shop-msg: cannot connect to Postgres at DSN {dsn!r}.\n"
            f"Check that the service is running (e.g. 'docker compose up -d').\n"
            f"Original error: {exc}"
        ) from exc
    _ensure_schema(conn)
    return conn


_DDL = """
CREATE TABLE IF NOT EXISTS messages (
  id           BIGSERIAL PRIMARY KEY,
  bc           TEXT NOT NULL,
  work_id      TEXT NOT NULL,
  direction    TEXT NOT NULL CHECK (direction IN ('inbox','outbox')),
  message_type TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (bc, work_id, direction, message_type)
);
"""


def _ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
    conn.commit()


# ---------------------------------------------------------------------------
# Collision error
# ---------------------------------------------------------------------------


class CollisionError(Exception):
    """Raised when an INSERT would violate the UNIQUE constraint."""


# ---------------------------------------------------------------------------
# bc identifier helpers
# ---------------------------------------------------------------------------


def _bc_id(bc_root: str) -> str:
    """Return the bc identifier for a bc_root path.

    The bc column in the messages table is the bc_root path string
    (str(Path(...))). Using the full path keeps each test's tmp_path
    isolated from other tests without any coordination.
    """
    return bc_root


def _bc_slug(bc_root: str) -> str:
    """Return a PostgreSQL identifier-safe slug for use in NOTIFY channel names.

    NOTIFY channel names may not contain certain characters; we replace
    non-alphanumeric characters with underscores and cap length at 63
    bytes (Postgres identifier limit).
    """
    slug = re.sub(r"[^a-zA-Z0-9]", "_", bc_root)
    return f"inbox_{slug}"[:63]


# ---------------------------------------------------------------------------
# Public storage API
# ---------------------------------------------------------------------------


def insert_message(
    bc_root: str,
    work_id: str,
    direction: str,
    message_type: str,
    payload: dict[str, Any],
    *,
    notify: bool = False,
) -> None:
    """Insert a message row into the messages table.

    Raises CollisionError if:
    - For inbox direction: any row already exists for (bc, work_id, 'inbox')
      regardless of message_type. A BC receives exactly one message per work_id.
    - For outbox direction: a row with the same (bc, work_id, 'outbox',
      message_type) already exists. The UNIQUE constraint handles this.

    When `notify=True` (inbox inserts), fires NOTIFY after the commit
    so agents listening on the channel wake up.
    """
    bc = _bc_id(bc_root)
    payload_json = json.dumps(payload)
    with _connect() as conn:
        with conn.cursor() as cur:
            # For inbox: enforce work_id uniqueness regardless of message_type.
            # A lead sends exactly one message per work_id into any BC's inbox.
            if direction == "inbox":
                cur.execute(
                    """
                    SELECT 1 FROM messages
                    WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                    LIMIT 1
                    """,
                    (bc, work_id),
                )
                if cur.fetchone() is not None:
                    raise CollisionError(
                        f"inbox message already exists: bc={bc_root!r} "
                        f"work_id={work_id!r}"
                    )

            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (bc, work_id, direction, message_type) DO NOTHING
                """,
                (bc, work_id, direction, message_type, payload_json),
            )
            rows_affected = cur.rowcount
        conn.commit()

    if rows_affected == 0:
        raise CollisionError(
            f"message already exists: bc={bc_root!r} work_id={work_id!r} "
            f"direction={direction!r} message_type={message_type!r}"
        )

    if notify and direction == "inbox":
        # Fire NOTIFY on a separate autocommit connection so the
        # notification is delivered even if the caller does not hold the
        # connection open. The channel name is the bc slug; the payload
        # is just the work_id (stays well under the 8KB NOTIFY cap).
        # NOTIFY requires a literal channel name (identifier) — it does
        # not accept parameterized channel names — so we format it as a
        # quoted identifier using psycopg's sql.Identifier to prevent
        # injection while keeping the syntax valid.
        from psycopg import sql
        channel = _bc_slug(bc_root)
        with psycopg.connect(_get_dsn(), autocommit=True) as nconn:
            nconn.execute(
                sql.SQL("NOTIFY {channel}, {payload}").format(
                    channel=sql.Identifier(channel),
                    payload=sql.Literal(work_id),
                )
            )


def query_pending_inbox(bc_root: str) -> list[tuple[str, str]]:
    """Return (work_id, message_type) pairs for unprocessed inbox messages.

    A message is 'pending' iff its inbox row has no corresponding outbox
    row for the same (bc, work_id). The query mirrors the directory-glob
    approach the file-based backend used.
    """
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.work_id, i.message_type
                FROM messages i
                WHERE i.bc = %s AND i.direction = 'inbox'
                  AND NOT EXISTS (
                    SELECT 1 FROM messages o
                    WHERE o.bc = i.bc
                      AND o.work_id = i.work_id
                      AND o.direction = 'outbox'
                  )
                ORDER BY i.created_at
                """,
                (bc,),
            )
            return [(row["work_id"], row["message_type"]) for row in cur.fetchall()]


def query_pending_outbox(
    lead_root: str, bc_filter: str | None = None
) -> list[tuple[str, str, str]]:
    """Return (work_id, message_type, bc_name) triples for pending outbox rows.

    Lead-side counterpart to query_pending_inbox. 'Pending' here means an
    outbox row exists (the lead has not yet consumed/acted on it). We
    derive the bc_name from the bc column's trailing path component so it
    matches the directory name under repos/.

    When bc_filter is given, restrict to rows whose bc column ends with
    that name.
    """
    import os as _os

    # We key off `bc` values that start with the lead_root + "/repos/" prefix
    # (or any prefix for test bc paths).
    with _connect() as conn:
        with conn.cursor() as cur:
            if bc_filter is not None:
                # bc column is a full path; bc_name is the final component.
                cur.execute(
                    """
                    SELECT work_id, message_type, bc
                    FROM messages
                    WHERE direction = 'outbox'
                      AND bc LIKE %s
                    ORDER BY created_at
                    """,
                    (f"%/{bc_filter}",),
                )
            else:
                cur.execute(
                    """
                    SELECT work_id, message_type, bc
                    FROM messages
                    WHERE direction = 'outbox'
                    ORDER BY bc, created_at
                    """,
                )
            rows = cur.fetchall()

    result = []
    seen_bcs: set[str] = set()
    for row in rows:
        bc = row["bc"]
        bc_name = _os.path.basename(bc)
        # For the lead-side `pending outbox` command the intent is to only
        # see BCs that are siblings under repos/. We filter by checking
        # that the bc path sits under <lead_root>/repos/.
        repos_prefix = lead_root.rstrip("/") + "/repos/"
        if not bc.startswith(repos_prefix):
            continue
        result.append((row["work_id"], row["message_type"], bc_name))
    return result


def read_inbox_message(bc_root: str, work_id: str) -> dict[str, Any] | None:
    """Return the payload dict for an inbox message, or None if not found."""
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload FROM messages
                WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                LIMIT 1
                """,
                (bc, work_id),
            )
            row = cur.fetchone()
    if row is None:
        return None
    payload = row["payload"]
    # psycopg v3 with row_factory=dict_row returns JSONB as a Python
    # dict already (the binary format deserializes it).
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def read_outbox_messages(bc_root: str, work_id: str) -> list[dict[str, Any]]:
    """Return all outbox payload dicts for a work_id, newest last."""
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, message_type FROM messages
                WHERE bc = %s AND work_id = %s AND direction = 'outbox'
                ORDER BY created_at
                """,
                (bc, work_id),
            )
            rows = cur.fetchall()
    result = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        result.append(payload)
    return result


def outbox_row_exists(bc_root: str, work_id: str, message_type: str) -> bool:
    """Return True iff an outbox row already exists for this bc/work_id/message_type."""
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM messages
                WHERE bc = %s AND work_id = %s
                  AND direction = 'outbox' AND message_type = %s
                LIMIT 1
                """,
                (bc, work_id, message_type),
            )
            return cur.fetchone() is not None


def inbox_row_exists(bc_root: str, work_id: str) -> bool:
    """Return True iff any inbox row exists for this bc/work_id."""
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM messages
                WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                LIMIT 1
                """,
                (bc, work_id),
            )
            return cur.fetchone() is not None


def delete_bc_messages(bc_root: str) -> None:
    """Delete all messages for a bc_root. Used for test teardown."""
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM messages WHERE bc = %s", (bc,))
        conn.commit()


def insert_raw_payload(
    bc_root: str,
    work_id: str,
    direction: str,
    message_type: str,
    payload: dict[str, Any],
) -> None:
    """Insert a raw (unvalidated) payload. Used by test setup steps that
    need to inject invalid payloads for schema-validation error paths.
    Does NOT fire NOTIFY. Raises CollisionError on duplicate.
    """
    insert_message(bc_root, work_id, direction, message_type, payload, notify=False)
