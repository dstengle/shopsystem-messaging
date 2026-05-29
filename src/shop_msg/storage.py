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
  work_id as the payload. After every outbox INSERT (from the respond
  commands), NOTIFY is fired on the channel `outbox_<bc_slug>` carrying
  the work_id as the payload. These are fire-and-forget from the storage
  layer's perspective; agents hold long-lived LISTEN connections
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

_DDL_CONSUMED_COL = """
ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS consumed BOOLEAN NOT NULL DEFAULT FALSE;
"""

_DDL_REGISTRY = """
CREATE TABLE IF NOT EXISTS shop_registry (
  name         TEXT PRIMARY KEY,
  shop_root    TEXT NOT NULL,
  shop_type    TEXT NOT NULL DEFAULT 'bc',
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
        cur.execute(_DDL_CONSUMED_COL)
        cur.execute(_DDL_REGISTRY)
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


def _bc_outbox_slug(bc_root: str) -> str:
    """Return a PostgreSQL identifier-safe slug for the outbox NOTIFY channel.

    Mirrors _bc_slug but prefixed with 'outbox_' so lead-side watchers can
    LISTEN for outbox events without colliding with inbox channels.
    """
    slug = re.sub(r"[^a-zA-Z0-9]", "_", bc_root)
    return f"outbox_{slug}"[:63]


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
    allow_multi_type: bool = False,
) -> None:
    """Insert a message row into the messages table.

    Raises CollisionError if:
    - For inbox direction with allow_multi_type=False (default): any row already
      exists for (bc, work_id, 'inbox') regardless of message_type.  A lead sends
      exactly one message per work_id into any BC's inbox.
    - For inbox direction with allow_multi_type=True: only raises if a row with the
      same (bc, work_id, 'inbox', message_type) already exists.  This path is used
      by BC-to-lead writes where multiple message_types (work_done, clarify,
      mechanism_observation) may land in the lead inbox for the same work_id.
    - For outbox direction: a row with the same (bc, work_id, 'outbox',
      message_type) already exists. The UNIQUE constraint handles this.

    When `notify=True` and direction='inbox', fires NOTIFY on the inbox
    channel after the commit so BC-side watchers wake up.
    When `notify=True` and direction='outbox', fires NOTIFY on the outbox
    channel after the commit so lead-side watchers wake up.
    """
    bc = _bc_id(bc_root)
    payload_json = json.dumps(payload)
    with _connect() as conn:
        with conn.cursor() as cur:
            # For inbox: enforce uniqueness.
            # When allow_multi_type=False (the default, used by lead-to-BC sends):
            #   a lead sends exactly one message per work_id, so any existing inbox
            #   row for (bc, work_id) raises CollisionError regardless of message_type.
            # When allow_multi_type=True (used by BC-to-lead writes):
            #   multiple message_types are permitted for the same work_id; only a
            #   duplicate (bc, work_id, direction, message_type) raises CollisionError,
            #   which the DB UNIQUE constraint enforces via ON CONFLICT DO NOTHING below.
            if direction == "inbox" and not allow_multi_type:
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

    if notify and direction == "outbox":
        # Fire NOTIFY on the outbox channel so lead-side watchers
        # (shop-msg watch --lead-root) can observe BC responses in
        # real time without polling.  The channel name is the outbox
        # slug; the payload is the work_id.
        from psycopg import sql
        channel = _bc_outbox_slug(bc_root)
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
                      AND consumed = FALSE
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
                      AND consumed = FALSE
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


def consume_outbox_message(bc_root: str, work_id: str, message_type: str) -> bool:
    """Mark a specific outbox row as consumed so it no longer appears in pending output.

    Returns True if the row was found and marked consumed, False if no matching
    unconsumed outbox row exists. Does not raise on missing rows; the CLI layer
    translates False into a non-zero exit with a descriptive error.
    """
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages
                SET consumed = TRUE
                WHERE bc = %s AND work_id = %s
                  AND direction = 'outbox' AND message_type = %s
                  AND consumed = FALSE
                """,
                (bc, work_id, message_type),
            )
            rows_affected = cur.rowcount
        conn.commit()
    return rows_affected > 0


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
    *,
    allow_multi_type: bool = False,
) -> None:
    """Insert a raw (unvalidated) payload. Used by test setup steps that
    need to inject invalid payloads for schema-validation error paths.
    Does NOT fire NOTIFY. Raises CollisionError on duplicate.

    When allow_multi_type=True, multiple message_types are permitted for the
    same (bc, work_id, direction='inbox') — matching the semantics of
    BC-to-lead inbox writes where work_done, clarify, and mechanism_observation
    may all arrive for the same work_id.
    """
    insert_message(
        bc_root,
        work_id,
        direction,
        message_type,
        payload,
        notify=False,
        allow_multi_type=allow_multi_type,
    )


def list_bc_roots_for_lead(lead_root: str) -> list[str]:
    """Return the list of BC root paths under <lead_root>/repos/.

    Each subdirectory of <lead_root>/repos/ that exists is treated as a BC.
    Returns full absolute paths suitable for use as bc identifiers.
    """
    import os as _os
    from pathlib import Path as _Path

    repos_dir = _Path(lead_root) / "repos"
    if not repos_dir.is_dir():
        return []
    return sorted(
        str(p.resolve())
        for p in repos_dir.iterdir()
        if p.is_dir()
    )


def watch_outbox_for_lead(lead_root: str) -> None:
    """Lead-side outbox watcher: drain pending outbox rows then LISTEN for new ones.

    Each output line is of the form:
        <work_id> <message_type>

    Startup sequence:
      1. Connect to Postgres (raises RuntimeError if unreachable).
      2. Discover BC roots under <lead_root>/repos/.
      3. LISTEN on each BC's outbox channel.
      4. Drain: query pending outbox rows across all known BCs and print each.
      5. Emit a sentinel READY line.
      6. Block on NOTIFY, printing one line per notification received.

    The function never returns under normal operation.
    """
    import sys
    from psycopg import sql

    bc_roots = list_bc_roots_for_lead(lead_root)
    # channel_to_bc maps each outbox channel name back to the bc_root path
    # so we can look up the message_type on notification.
    channel_to_bc: dict[str, str] = {
        _bc_outbox_slug(bc): bc for bc in bc_roots
    }

    dsn = _get_dsn()
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        raise RuntimeError(
            f"shop-msg watch: cannot connect to Postgres at DSN {dsn!r}.\n"
            f"Check that the service is running (e.g. 'docker compose up -d').\n"
            f"Original error: {exc}"
        ) from exc

    try:
        sys.stdout.reconfigure(line_buffering=True)

        _ensure_schema(conn)

        # LISTEN on all known BC outbox channels.
        for channel in channel_to_bc:
            conn.execute(
                sql.SQL("LISTEN {channel}").format(channel=sql.Identifier(channel))
            )

        # Drain existing outbox rows across all known BCs.
        if bc_roots:
            bc_ids = [_bc_id(bc) for bc in bc_roots]
            placeholders = ", ".join(["%s"] * len(bc_ids))
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT bc, work_id, message_type
                    FROM messages
                    WHERE bc IN ({placeholders}) AND direction = 'outbox'
                    ORDER BY created_at
                    """,
                    bc_ids,
                )
                for row in cur.fetchall():
                    print(f"{row['work_id']} {row['message_type']}")

        print("READY")

        # Block indefinitely on NOTIFY.
        for notify in conn.notifies():
            work_id = notify.payload
            bc_root_for_notify = channel_to_bc.get(notify.channel)
            if bc_root_for_notify is None:
                continue
            bc_for_notify = _bc_id(bc_root_for_notify)
            with _connect() as lookup_conn:
                with lookup_conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        SELECT message_type FROM messages
                        WHERE bc = %s AND work_id = %s AND direction = 'outbox'
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (bc_for_notify, work_id),
                    )
                    row = cur.fetchone()
            message_type = row["message_type"] if row else "unknown"
            print(f"{work_id} {message_type}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Shop registry (name-based addressing — PDR-007 / Brief-006)
# ---------------------------------------------------------------------------


def registry_add(name: str, shop_root: str, shop_type: str = "bc") -> None:
    """Register a shop by canonical name.

    If the name already exists with the same shop_root and shop_type, this
    is a no-op (idempotent). If the name exists with different values, the
    existing entry is updated (upsert semantics).
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shop_registry (name, shop_root, shop_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                  SET shop_root = EXCLUDED.shop_root,
                      shop_type = EXCLUDED.shop_type
                """,
                (name, shop_root, shop_type),
            )
        conn.commit()


def registry_remove(name: str) -> bool:
    """Remove a shop from the registry by canonical name.

    Returns True if an entry was removed, False if the name was not found.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM shop_registry WHERE name = %s",
                (name,),
            )
            rows_affected = cur.rowcount
        conn.commit()
    return rows_affected > 0


def registry_list() -> list[tuple[str, str, str]]:
    """Return all registry entries as (name, shop_root, shop_type) triples."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, shop_root, shop_type
                FROM shop_registry
                ORDER BY name
                """
            )
            return [(row["name"], row["shop_root"], row["shop_type"]) for row in cur.fetchall()]


def registry_sync(manifest_path: str) -> None:
    """Synchronise the registry from a BC manifest file.

    The manifest is a YAML or JSON file with a top-level 'bcs' key mapping
    canonical BC names to their shop_root paths. Entries in the manifest
    are upserted; entries absent from the manifest (except lead shops) are
    removed. Lead shops (shop_type='lead') are never removed by sync.

    Manifest format (YAML or JSON):
        bcs:
          shopsystem-messaging: /path/to/repo
          shopsystem-scenarios: /path/to/repo
    """
    import json as _json
    from pathlib import Path as _Path
    import yaml as _yaml

    raw = _Path(manifest_path).read_text()
    try:
        data = _yaml.safe_load(raw)
    except Exception:
        data = _json.loads(raw)

    manifest_bcs: dict[str, str] = data.get("bcs", {}) or {}

    with _connect() as conn:
        with conn.cursor() as cur:
            # Upsert all BCs from manifest.
            for name, shop_root in manifest_bcs.items():
                cur.execute(
                    """
                    INSERT INTO shop_registry (name, shop_root, shop_type)
                    VALUES (%s, %s, 'bc')
                    ON CONFLICT (name) DO UPDATE
                      SET shop_root = EXCLUDED.shop_root,
                          shop_type = 'bc'
                    """,
                    (name, shop_root),
                )
            # Remove BC entries not in manifest (do not remove lead entries).
            if manifest_bcs:
                placeholders = ", ".join(["%s"] * len(manifest_bcs))
                cur.execute(
                    f"""
                    DELETE FROM shop_registry
                    WHERE shop_type = 'bc'
                      AND name NOT IN ({placeholders})
                    """,
                    list(manifest_bcs.keys()),
                )
            else:
                # Empty manifest: remove all BC entries.
                cur.execute(
                    "DELETE FROM shop_registry WHERE shop_type = 'bc'"
                )
        conn.commit()


def resolve_shop_name(name: str) -> str | None:
    """Resolve a canonical shop name to its shop_root path.

    Returns the shop_root string if found, or None if the name is not
    registered.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT shop_root FROM shop_registry WHERE name = %s",
                (name,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return row["shop_root"]


def listen_on_outbox_channel(bc_root: str, timeout: float = 5.0) -> list[str]:
    """LISTEN on the outbox channel for bc_root and return received payloads.

    Used by BDD test step definitions to verify that an outbox NOTIFY is fired.
    Connects, LISTENs, waits up to `timeout` seconds for any notification,
    and returns a list of payload strings received.

    This is a test-support helper and not part of the production CLI surface.
    """
    from psycopg import sql

    channel = _bc_outbox_slug(bc_root)
    dsn = _get_dsn()
    payloads: list[str] = []

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("LISTEN {channel}").format(channel=sql.Identifier(channel))
        )
        for notify in conn.notifies(timeout=timeout):
            payloads.append(notify.payload)
            break  # return after first notification

    return payloads


def resolve_lead_shop() -> str | None:
    """Return the shop_root for the first registered lead shop, or None.

    Used by ``shop-msg respond`` to determine where to route the response:
    the lead shop's inbox receives all BC responses (Brief-006 scope C).
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT shop_root FROM shop_registry
                WHERE shop_type = 'lead'
                ORDER BY name
                LIMIT 1
                """
            )
            row = cur.fetchone()
    if row is None:
        return None
    return row["shop_root"]


def _lead_inbox_slug(lead_root: str) -> str:
    """Return a Postgres-safe NOTIFY channel name for a lead shop's inbox.

    Uses the same naming scheme as _bc_slug (inbox_ prefix + slug) so
    the lead's inbox channel is indistinguishable in form from any BC's
    inbox channel — it is distinguished by the lead's root path slug.
    """
    slug = re.sub(r"[^a-zA-Z0-9]", "_", lead_root)
    return f"inbox_{slug}"[:63]


def existing_lead_inbox_message_type(
    lead_root: str, work_id: str, message_type: str
) -> str | None:
    """Return the message_type of an existing lead-inbox row for this triple, or None.

    Used by the CLI to enrich the collision error message (lead-b3z) with the
    existing row's message_type before reporting refusal. The triple keyed on
    is (bc=lead_root, direction='inbox', work_id, message_type) — identical to
    the SELECT gate in ``insert_bc_response`` — so a hit means a collision
    would occur on a fresh (non-force) insert.
    """
    lead_bc_id = _bc_id(lead_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT message_type FROM messages
                WHERE bc = %s AND work_id = %s
                  AND direction = 'inbox' AND message_type = %s
                LIMIT 1
                """,
                (lead_bc_id, work_id, message_type),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return row["message_type"]


def insert_bc_response(
    lead_root: str,
    bc_root: str,
    work_id: str,
    message_type: str,
    payload: dict[str, Any],
    *,
    force: bool = False,
) -> None:
    """Insert a BC response into the lead shop's inbox namespace.

    This replaces the old ``insert_message(..., direction='outbox', ...)``
    call that the respond commands previously used.  Under the new routing
    (Brief-006 scope C / PDR-007 / lead-e9x) BC responses are delivered
    TO the lead's inbox rather than written to the BC's own outbox.

    Two writes happen atomically:
    1. ``(bc=lead_root, work_id, direction='inbox', message_type)`` — the
       response in the lead's namespace.  This is the primary delivery target
       and the row that ``shop-msg read inbox --lead`` and
       ``shop-msg pending inbox --lead`` inspect.
    2. ``(bc=bc_root, work_id, direction='outbox', message_type)`` — a local
       marker in the BC's own namespace so that ``shop-msg pending inbox --bc``
       can determine that the BC has responded to this work_id (the existing
       ``NOT EXISTS outbox`` query in ``query_pending_inbox`` uses this).

    Collision semantics: if the lead-inbox row already exists, ``CollisionError``
    is raised (no second write attempted).  The BC-side marker is written
    idempotently (ON CONFLICT DO NOTHING) since it is a secondary artefact.

    Recovery path (``force=True``, lead-2id): the existing lead-inbox row
    matching the (bc=lead_root, direction='inbox', work_id, message_type)
    triple is DELETEd in the SAME transaction as the replacement INSERT, so
    the new payload becomes the surviving delivered response.  The DELETE is
    scoped to exactly that triple (per-message_type), matching the SELECT gate;
    a different message_type's row on the same work_id is untouched.  The
    BC-side marker is re-written so it carries the replacement payload too.
    NOTIFY still fires on the force path so ``shop-msg watch --lead`` wakes for
    the replacement.

    After a successful insert, fires NOTIFY on the lead-inbox channel so
    ``shop-msg watch --lead`` can wake up.
    """
    lead_bc_id = _bc_id(lead_root)
    bc_bc_id = _bc_id(bc_root)
    payload_json = json.dumps(payload)

    with _connect() as conn:
        with conn.cursor() as cur:
            if force:
                # Recovery path: atomically replace the existing triple.
                # DELETE the prior lead-inbox row (scoped per-message_type) so
                # the replacement INSERT lands cleanly in the same transaction.
                cur.execute(
                    """
                    DELETE FROM messages
                    WHERE bc = %s AND work_id = %s
                      AND direction = 'inbox' AND message_type = %s
                    """,
                    (lead_bc_id, work_id, message_type),
                )
                # Re-write the BC-side marker as well so it carries the
                # replacement payload rather than a stale one.
                cur.execute(
                    """
                    DELETE FROM messages
                    WHERE bc = %s AND work_id = %s
                      AND direction = 'outbox' AND message_type = %s
                    """,
                    (bc_bc_id, work_id, message_type),
                )
            else:
                # Check collision on the lead-inbox row first (primary target).
                cur.execute(
                    """
                    SELECT 1 FROM messages
                    WHERE bc = %s AND work_id = %s
                      AND direction = 'inbox' AND message_type = %s
                    LIMIT 1
                    """,
                    (lead_bc_id, work_id, message_type),
                )
                if cur.fetchone() is not None:
                    raise CollisionError(
                        f"response already exists: lead={lead_root!r} work_id={work_id!r} "
                        f"message_type={message_type!r}"
                    )

            # 1. Write to lead inbox.
            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, 'inbox', %s, %s::jsonb)
                ON CONFLICT (bc, work_id, direction, message_type) DO NOTHING
                """,
                (lead_bc_id, work_id, message_type, payload_json),
            )
            lead_rows = cur.rowcount

            # 2. Write BC-side marker (for pending inbox --bc tracking).
            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, 'outbox', %s, %s::jsonb)
                ON CONFLICT (bc, work_id, direction, message_type) DO NOTHING
                """,
                (bc_bc_id, work_id, message_type, payload_json),
            )
        conn.commit()

    if lead_rows == 0:
        raise CollisionError(
            f"response already exists: lead={lead_root!r} work_id={work_id!r} "
            f"message_type={message_type!r}"
        )

    # Fire NOTIFY on the lead's inbox channel so ``watch --lead`` wakes up.
    from psycopg import sql
    channel = _lead_inbox_slug(lead_root)
    with psycopg.connect(_get_dsn(), autocommit=True) as nconn:
        nconn.execute(
            sql.SQL("NOTIFY {channel}, {payload}").format(
                channel=sql.Identifier(channel),
                payload=sql.Literal(work_id),
            )
        )


def query_pending_lead_inbox(lead_root: str) -> list[tuple[str, str]]:
    """Return (work_id, message_type) pairs for BC responses in the lead's inbox.

    A BC response is 'pending' (from the lead's perspective) if it exists
    in the lead's inbox namespace and has not been marked consumed.
    """
    bc = _bc_id(lead_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT work_id, message_type
                FROM messages
                WHERE bc = %s AND direction = 'inbox'
                  AND consumed = FALSE
                ORDER BY created_at
                """,
                (bc,),
            )
            return [(row["work_id"], row["message_type"]) for row in cur.fetchall()]


def read_lead_inbox_message(lead_root: str, work_id: str) -> dict[str, Any] | None:
    """Return the most recent BC-response payload for work_id in the lead's inbox.

    Returns None if no matching row exists.
    """
    bc = _bc_id(lead_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload FROM messages
                WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (bc, work_id),
            )
            row = cur.fetchone()
    if row is None:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


# ---------------------------------------------------------------------------
# LISTEN-drop reconnect (lead-m32 / supersedes lead-7v1)
#
# The long-running Monitor surfaces (watch_lead_inbox, watch_inbox) block on a
# bare `for notify in conn.notifies():` loop. If the underlying connection is
# dropped (DB container restart, network hiccup, keepalive expiry) the bare
# loop silently stops delivering notifications while the process stays alive —
# the lead-7v1 bug. The hybrid contract: bounded reconnect with per-attempt
# stdout logging, then a non-zero exit with stderr on exhaustion.
# ---------------------------------------------------------------------------

# Bounded reconnect params: max 5 attempts, exponential backoff starting at 1s
# (1, 2, 4, 8, 16). Round numbers chosen as a simple test seam.
_LISTEN_MAX_ATTEMPTS = 5
_LISTEN_BACKOFF_BASE = 1


def _listen_backoff_seconds(attempt: int) -> int:
    """Backoff for a 1-indexed reconnect attempt: 1, 2, 4, 8, 16."""
    return _LISTEN_BACKOFF_BASE * (2 ** (attempt - 1))


def _sleep(seconds: float) -> None:
    """Sleep seam — overridable in tests so reconnect backoff is instant."""
    import time as _time

    _time.sleep(seconds)


def _reconfigure_stdout_line_buffered() -> None:
    """Force line-buffering on stdout when supported.

    No-op when stdout does not support reconfigure() (e.g. an in-process
    StringIO under redirect_stdout in the test harness), so the watcher
    surfaces remain directly callable in-process.
    """
    import sys

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure is not None:
        reconfigure(line_buffering=True)


def _open_listen_connection(channel: str):
    """Open a fresh autocommit connection and LISTEN on ``channel``.

    Factored out so the reconnect path and the initial connect path share one
    implementation, and so tests can monkeypatch the reconnect seam.
    """
    from psycopg import sql

    dsn = _get_dsn()
    conn = psycopg.connect(dsn, autocommit=True)
    conn.execute(sql.SQL("LISTEN {channel}").format(channel=sql.Identifier(channel)))
    return conn


# Connection-loss signals collapsed into one handler. notifies() may raise
# psycopg.OperationalError directly, or exhaust (StopIteration, surfaced as a
# normal loop exit) with conn.broken True, or raise IOError on a dead socket.
_LISTEN_DROP_EXCEPTIONS = (psycopg.OperationalError, IOError)


def _run_listen_loop_with_reconnect(conn, channel: str, handle_notify) -> None:
    """Run the live NOTIFY loop with bounded reconnect.

    ``conn`` is the already-LISTENing connection (post-drain). ``handle_notify``
    is called with each ``notify`` object. On a connection drop this re-opens a
    fresh LISTEN connection and resumes the bare notifies() loop ONLY — it never
    re-runs the caller's drain phase, so already-handled work_ids are not
    re-emitted (the lead-m32 no-re-drain constraint).
    """
    import sys

    while True:
        dropped = False
        try:
            for notify in conn.notifies():
                handle_notify(notify)
            # notifies() returned (generator exhausted). A clean exhaustion
            # with a broken connection is a drop; otherwise treat as drop too,
            # since the long-running loop is not expected to terminate normally.
            dropped = True
        except _LISTEN_DROP_EXCEPTIONS:
            dropped = True

        if not dropped:
            return

        # Reconnect with bounded exponential backoff.
        try:
            conn.close()
        except Exception:
            pass

        reconnected = False
        for attempt in range(1, _LISTEN_MAX_ATTEMPTS + 1):
            backoff = _listen_backoff_seconds(attempt)
            print(
                f"LISTEN_DROP attempt={attempt}/{_LISTEN_MAX_ATTEMPTS} "
                f"backoff={backoff}s"
            )
            _sleep(backoff)
            try:
                conn = _open_listen_connection(channel)
            except _LISTEN_DROP_EXCEPTIONS:
                continue
            print("LISTEN_RECONNECTED")
            reconnected = True
            break

        if not reconnected:
            print(
                f"error: LISTEN watcher could not reconnect after "
                f"{_LISTEN_MAX_ATTEMPTS} attempts; exiting",
                file=sys.stderr,
            )
            sys.exit(2)
        # Loop back to resume the bare notifies() loop on the new conn.


def watch_lead_inbox(lead_root: str) -> None:
    """Lead-side inbox watcher: drain pending BC responses then LISTEN for new ones.

    This replaces ``watch_outbox_for_lead`` under the new routing model
    (Brief-006 scope E): the lead watches its own inbox channel instead
    of polling BC outbox channels.

    Each output line is of the form:
        <work_id> <message_type>

    Startup sequence:
      1. Connect to Postgres.
      2. LISTEN on the lead's inbox channel.
      3. Drain: query pending BC responses (direction='inbox', consumed=FALSE)
         and print each one.
      4. Emit a sentinel READY line.
      5. Block on NOTIFY, printing one line per notification received.

    The function never returns under normal operation.
    """
    import sys
    from psycopg import sql

    bc = _bc_id(lead_root)
    channel = _lead_inbox_slug(lead_root)

    dsn = _get_dsn()
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        raise RuntimeError(
            f"shop-msg watch: cannot connect to Postgres at DSN {dsn!r}.\n"
            f"Check that the service is running (e.g. 'docker compose up -d').\n"
            f"Original error: {exc}"
        ) from exc

    try:
        _reconfigure_stdout_line_buffered()

        _ensure_schema(conn)

        conn.execute(
            sql.SQL("LISTEN {channel}").format(channel=sql.Identifier(channel))
        )

        # Drain existing BC responses in the lead's inbox.
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT work_id, message_type
                FROM messages
                WHERE bc = %s AND direction = 'inbox'
                  AND consumed = FALSE
                ORDER BY created_at
                """,
                (bc,),
            )
            for row in cur.fetchall():
                print(f"{row['work_id']} {row['message_type']}")

        print("READY")

        # Block indefinitely on NOTIFY, with bounded reconnect on drop.
        def _handle(notify):
            work_id = notify.payload
            # Look up message_type from the DB.
            with _connect() as lookup_conn:
                with lookup_conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        SELECT message_type FROM messages
                        WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (bc, work_id),
                    )
                    row = cur.fetchone()
            message_type = row["message_type"] if row else "unknown"
            print(f"{work_id} {message_type}")

        _run_listen_loop_with_reconnect(conn, channel, _handle)
    finally:
        conn.close()


def listen_on_lead_inbox_channel(lead_root: str, timeout: float = 5.0) -> list[str]:
    """LISTEN on the lead's inbox channel and return received payloads.

    Test-support helper for BDD step definitions that verify a NOTIFY
    is fired when a BC executes ``shop-msg respond``.
    """
    from psycopg import sql

    channel = _lead_inbox_slug(lead_root)
    dsn = _get_dsn()
    payloads: list[str] = []

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("LISTEN {channel}").format(channel=sql.Identifier(channel))
        )
        for notify in conn.notifies(timeout=timeout):
            payloads.append(notify.payload)
            break  # return after first notification

    return payloads


def watch_inbox(bc_root: str) -> None:
    """Drain pending inbox messages then LISTEN for new ones, printing one line
    per event to stdout.

    Each output line is of the form:
        <work_id> <message_type>

    This is the Monitor-compatible format: the harness can wake agent
    processes by reading these lines from stdout.

    Startup sequence:
      1. Connect to Postgres (raises RuntimeError via _connect if unreachable).
      2. LISTEN on the BC's inbox channel.
      3. Drain: query pending inbox rows and print each one.
      4. Emit a sentinel READY line so callers can detect drain completion.
      5. Block on NOTIFY, printing one line per notification received.

    The function never returns under normal operation; it loops forever
    waiting for notifications.
    """
    import sys
    from psycopg import sql

    bc = _bc_id(bc_root)
    channel = _bc_slug(bc_root)

    # Step 1 & 2: connect and LISTEN before draining so we cannot miss a
    # notification that fires between the drain query and the LISTEN.
    # We use autocommit=True for the LISTEN connection; LISTEN requires it.
    dsn = _get_dsn()
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        raise RuntimeError(
            f"shop-msg watch: cannot connect to Postgres at DSN {dsn!r}.\n"
            f"Check that the service is running (e.g. 'docker compose up -d').\n"
            f"Original error: {exc}"
        ) from exc

    try:
        # Force line-buffering on stdout so each print() reaches the pipe
        # immediately even when stdout is not a TTY (e.g. when a subprocess
        # Popen captures it).  Python switches to block-buffering for
        # non-TTY stdout; `flush=True` on print() only flushes Python's
        # internal layer, not the underlying C-level buffer.  Reconfiguring
        # to line_buffering=True ensures every newline flushes through.
        _reconfigure_stdout_line_buffered()

        _ensure_schema(conn)

        conn.execute(
            sql.SQL("LISTEN {channel}").format(channel=sql.Identifier(channel))
        )

        # Step 3: drain pending inbox messages.
        with conn.cursor(row_factory=dict_row) as cur:
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
            for row in cur.fetchall():
                print(f"{row['work_id']} {row['message_type']}")

        # Step 4: sentinel so callers can detect drain completion.
        print("READY")

        # Step 5: block indefinitely on NOTIFY, with bounded reconnect on drop.
        def _handle(notify):
            # The notification payload is the work_id.
            work_id = notify.payload
            # Look up the message_type from the DB using a separate connection.
            # We MUST NOT use the LISTEN connection (conn) here because
            # conn.notifies() holds conn.lock for the duration of the generator.
            # Calling conn.cursor().execute() inside the loop body would try to
            # re-acquire the same non-reentrant lock, causing a deadlock.
            with _connect() as lookup_conn:
                with lookup_conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        SELECT message_type FROM messages
                        WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (bc, work_id),
                    )
                    row = cur.fetchone()
            message_type = row["message_type"] if row else "unknown"
            print(f"{work_id} {message_type}")

        _run_listen_loop_with_reconnect(conn, channel, _handle)
    finally:
        conn.close()
