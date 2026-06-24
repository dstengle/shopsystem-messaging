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
    direction   TEXT NOT NULL CHECK (direction IN ('inbox','outbox','nudge')),
    message_type TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
  );

Uniqueness (lead-xp5f / ADR-015 nudge direction):
  The one-message-per-(bc,work_id,direction,message_type) invariant that
  inbox/outbox collision detection (respond collision/--force, consume)
  depends on is enforced by a PARTIAL unique index scoped to direction IN
  ('inbox','outbox') ONLY:

    CREATE UNIQUE INDEX messages_inbox_outbox_uq
      ON messages (bc, work_id, direction, message_type)
      WHERE direction IN ('inbox','outbox');

  direction='nudge' rows are deliberately OUTSIDE that index, so a second
  (or Nth) nudge against the same (bc, work_id) is storable — a nudge is
  auxiliary signaling, not subject to the dispatch lifecycle (ADR-015
  decision 6 / lead-1w7r decision 1). The discriminator distinguishing
  multiple nudges is the BIGSERIAL ``id`` + ``created_at``. The partial
  index is scoped so inbox/outbox collision/--force/consume behavior is
  byte-for-byte preserved.

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


def probe_db_reachable() -> None:
    """Open and close a connection to verify the registry DB is reachable.

    Used by ``shop-msg prime`` on the orient-without-a-resolved-shop path
    (lead-t8v8 scenario 48): when the CWD-derived shop name does not resolve
    against the registry there is no bc_root to query pending inbox rows for,
    but prime must still report DB reachability. Raises ``RuntimeError`` (via
    ``_connect``) when the DSN is unreachable, preserving the DB-unreachable
    hard-exit shape used elsewhere.
    """
    conn = _connect()
    conn.close()


_DDL = """
CREATE TABLE IF NOT EXISTS messages (
  id           BIGSERIAL PRIMARY KEY,
  bc           TEXT NOT NULL,
  work_id      TEXT NOT NULL,
  direction    TEXT NOT NULL CHECK (direction IN ('inbox','outbox','nudge')),
  message_type TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# Migration (lead-xp5f / ADR-015): widen the direction CHECK to admit
# 'nudge'. On a fresh DB the CREATE above already includes 'nudge'; on a
# pre-existing production table the original CHECK only allows
# ('inbox','outbox'), so we drop-and-recreate the constraint by name. Both
# the named constraint (older tables) and the anonymous form are handled by
# probing pg_constraint and recreating a known-named one. Idempotent.
_DDL_DIRECTION_CHECK_MIGRATION = """
DO $$
DECLARE
  conname text;
BEGIN
  -- Drop any existing CHECK constraint on the direction column whose
  -- definition does NOT already permit 'nudge'.
  FOR conname IN
    SELECT c.conname
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'messages'
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%direction%'
      AND pg_get_constraintdef(c.oid) NOT LIKE '%nudge%'
  LOOP
    EXECUTE format('ALTER TABLE messages DROP CONSTRAINT %I', conname);
  END LOOP;
  -- Ensure a widened named CHECK exists.
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'messages' AND c.conname = 'messages_direction_check_nudge'
  ) THEN
    ALTER TABLE messages
      ADD CONSTRAINT messages_direction_check_nudge
      CHECK (direction IN ('inbox','outbox','nudge'));
  END IF;
END $$;
"""

# Uniqueness migration (lead-xp5f / ADR-015): the inbox/outbox
# one-row-per-(bc,work_id,direction,message_type) invariant is enforced by a
# PARTIAL unique index scoped to direction IN ('inbox','outbox'). nudge rows
# are deliberately outside it (multi-delivery). On a pre-existing table the
# original table-level UNIQUE(bc,work_id,direction,message_type) covered ALL
# directions; we drop that anonymous/auto-named UNIQUE constraint (it would
# otherwise block a second nudge) and replace it with the partial index.
# Idempotent: re-running drops nothing once the table-wide UNIQUE is gone and
# CREATE UNIQUE INDEX IF NOT EXISTS is a no-op once present.
_DDL_PARTIAL_UNIQUE_MIGRATION = """
DO $$
DECLARE
  conname text;
BEGIN
  -- Drop any UNIQUE constraint over the full (bc,work_id,direction,message_type)
  -- tuple that is NOT scoped to inbox/outbox (i.e. the legacy table-wide one).
  FOR conname IN
    SELECT c.conname
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'messages'
      AND c.contype = 'u'
  LOOP
    EXECUTE format('ALTER TABLE messages DROP CONSTRAINT %I', conname);
  END LOOP;
END $$;
"""

_DDL_PARTIAL_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS messages_inbox_outbox_uq
  ON messages (bc, work_id, direction, message_type)
  WHERE direction IN ('inbox','outbox');
"""

_DDL_CONSUMED_COL = """
ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS consumed BOOLEAN NOT NULL DEFAULT FALSE;
"""

# lead-9xrd: the messaging audit trail. An append-only record of
# lifecycle events against a (bc, work_id, message_type) triple. The
# ``retract inbox`` command writes here on BOTH paths: a successful
# retraction records event='retracted'; a refused retraction (the deposit
# was already consumed) records event='retract_refused'. Keyed by triple +
# event + timestamp; multiple events for one triple are expected, so there
# is no uniqueness constraint (the append-only audit log is intentionally
# multi-row per triple).
_DDL_MESSAGE_AUDIT = """
CREATE TABLE IF NOT EXISTS message_audit (
  id           BIGSERIAL PRIMARY KEY,
  bc           TEXT NOT NULL,
  work_id      TEXT NOT NULL,
  message_type TEXT NOT NULL,
  event        TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# ADR-020 / PDR-007 Option A: the registry stores NO filesystem path. Each
# entry is keyed by canonical name and carries an abstract address
# (``<system>/<name>``; the lead collapses to the sentinel
# ``<system>/lead``) plus the shop_type. The fresh-table DDL omits
# shop_root entirely; the migration below drops a legacy shop_root column
# and backfills abstract_address from canonical names.
_DDL_REGISTRY = """
CREATE TABLE IF NOT EXISTS shop_registry (
  name             TEXT PRIMARY KEY,
  abstract_address TEXT NOT NULL,
  shop_type        TEXT NOT NULL DEFAULT 'bc',
  registered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# Deployment system slug used to project canonical names onto abstract
# addresses (ADR-020). A canonical BC name "shopsystem-messaging" projects
# to the abstract address "shopsystem/messaging"; the lead collapses to the
# sentinel "shopsystem/lead" regardless of its canonical name.
#
# DEFAULT_SYSTEM_SLUG is the deployment default and is what every projection
# resolves to when the slug is not externally configured (scenarios 50/53).
# It is NOT a fixed binding: lead-tgsb / PDR-018 gate condition #2 externalizes
# the slug so a genuinely-distinct second product obtains a distinct ADR-020
# identity end-to-end. The slug is derived from an explicit configuration
# surface following the established SHOPMSG_DSN pattern (see _get_system_slug).
DEFAULT_SYSTEM_SLUG = "shopsystem"

# Back-compat aliases. The legacy module constants SYSTEM_SLUG and
# LEAD_ABSTRACT_ADDRESS name the DEPLOYMENT-DEFAULT slug / lead sentinel
# (slug ``shopsystem`` / ``shopsystem/lead``). They are retained for
# tests/importers that reference the default by name (e.g. conftest's
# ``_lead_address`` helper, which runs on the default-path projection).
#
# Live projection no longer binds to these constants — both the lead
# sentinel and the addressing-migration backfill CASE call
# _get_system_slug() at OPERATION time (lead-ikp5) so the configured
# SHOPMSG_SYSTEM_SLUG flows through. A module-level constant captures the
# env value once at import; resolving inside the function/query-builder is
# what lets a configured slug take effect per operation.
SYSTEM_SLUG = DEFAULT_SYSTEM_SLUG
LEAD_ABSTRACT_ADDRESS = f"{DEFAULT_SYSTEM_SLUG}/lead"


def _lead_abstract_address() -> str:
    """Return the lead sentinel abstract address for the CONFIGURED slug.

    The lead collapses to ``<slug>/lead`` where ``<slug>`` is resolved from
    SHOPMSG_SYSTEM_SLUG at call time (defaulting to ``shopsystem``). This is
    the per-operation counterpart to the frozen ``LEAD_ABSTRACT_ADDRESS``
    constant; live code paths (the addressing-migration backfill) consult
    this so a configured slug takes effect (lead-ikp5).
    """
    return f"{_get_system_slug()}/lead"


def _get_system_slug() -> str:
    """Return the deployment system slug from the documented config surface.

    Precedence (option (c) in the lead-tgsb dispatch — the established
    SHOPMSG_DSN-style environment override):

      1. SHOPMSG_SYSTEM_SLUG, if set and non-empty after stripping.
      2. The deployment default ``shopsystem`` (DEFAULT_SYSTEM_SLUG).

    Mirrors _get_dsn(): a single env-var read with a default, so the slug is
    configured the same way the DSN is. When the override is unset (or empty)
    the default projection pinned by scenarios 50/53 holds unchanged.
    """
    raw = os.environ.get("SHOPMSG_SYSTEM_SLUG", "")
    slug = raw.strip()
    return slug if slug else DEFAULT_SYSTEM_SLUG


def _abstract_address_for(name: str, shop_type: str) -> str:
    """Project a (canonical-name, shop_type) pair onto its abstract address.

    A lead shop collapses to the sentinel ``<system>/lead`` regardless of its
    canonical name. A BC name of the form ``<system>-<rest>`` projects to
    ``<system>/<rest>``; a name carrying no recognizable system prefix is
    placed under the deployment system slug as ``<system>/<name>``.

    The ``<system>`` segment is the configured deployment slug
    (_get_system_slug) — by default ``shopsystem`` (scenarios 50/53), or the
    product's slug when SHOPMSG_SYSTEM_SLUG is set (PDR-018 gate #2).
    """
    slug = _get_system_slug()
    if shop_type == "lead":
        return f"{slug}/lead"
    prefix = f"{slug}-"
    if name.startswith(prefix):
        rest = name[len(prefix):]
        return f"{slug}/{rest}"
    return f"{slug}/{name}"


# ADR-020 addressing migration. A pre-migration shop_registry keyed on a
# path-bearing shape (a shop_root column) is migrated in place:
#   1. Add the abstract_address column if absent.
#   2. Backfill abstract_address from each row's canonical name + shop_type
#      (lead -> sentinel), dropping any row whose key cannot be mapped to a
#      known canonical name (an orphan path key such as "/workspace" or a
#      tmp_path-prefixed key has no canonical name and is removed).
#   3. Drop the legacy shop_root column so no entry retains a path.
# Idempotent: re-running finds abstract_address already present and shop_root
# already gone, and performs no row deletion the second time.
#
# lead-ikp5: the backfill CASE projection MUST consult the env-resolved slug
# at OPERATION time, not a constant frozen at import. The SQL is therefore
# built per-call by _registry_address_migration_sql() (run inside
# _ensure_schema on every _connect()) rather than interpolated once at import.
# Under SHOPMSG_SYSTEM_SLUG=dummyco the CASE projects dummyco/<rest> and the
# lead collapses to dummyco/lead; with the slug unset it projects
# shopsystem/<rest> / shopsystem/lead (non-regressing, scenarios 50/53/e9a31b6f).
def _registry_address_migration_sql() -> str:
    """Build the ADR-020 addressing-migration DO block for the CURRENT slug.

    The ``<system>`` segment of the backfill CASE and the lead sentinel are
    resolved from _get_system_slug() at call time so a configured
    SHOPMSG_SYSTEM_SLUG flows through. The slug is validated against a strict
    charset before interpolation (it is a deployment-config identifier, not
    user input, but interpolating into SQL warrants the guard); the prefix
    length is computed from the live slug so the ``substring`` offset matches.
    """
    slug = _get_system_slug()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", slug):
        raise ValueError(
            f"shop-msg: SHOPMSG_SYSTEM_SLUG {slug!r} is not a valid system slug "
            f"(allowed characters: letters, digits, '-', '_')."
        )
    lead_sentinel = f"{slug}/lead"
    prefix_offset = str(len(slug) + 2)
    return """
DO $$
BEGIN
  -- Step 1: ensure the abstract_address column exists.
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'shop_registry' AND column_name = 'abstract_address'
  ) THEN
    ALTER TABLE shop_registry ADD COLUMN abstract_address TEXT;
  END IF;

  -- Step 2: drop orphan rows whose key (name) cannot be mapped to a known
  -- canonical name. A canonical BC/lead name does not start with '/' and is
  -- not a pytest tmp_path key; any row whose name looks like a filesystem
  -- path is an orphan from the path-keyed era and is removed.
  DELETE FROM shop_registry
  WHERE name LIKE '/%' OR name LIKE '%/pytest-%' OR name LIKE '/tmp/%';

  -- Step 3: backfill abstract_address from the canonical name + shop_type for
  -- any row that does not yet carry one. The <system> segment is the
  -- env-resolved slug (lead-ikp5), not a frozen constant.
  UPDATE shop_registry
  SET abstract_address = CASE
    WHEN shop_type = 'lead' THEN '""" + lead_sentinel + """'
    WHEN name LIKE '""" + slug + """-%'
      THEN '""" + slug + """/' || substring(name from """ + prefix_offset + """)
    ELSE '""" + slug + """/' || name
  END
  WHERE abstract_address IS NULL;

  -- Step 4: enforce NOT NULL now that every surviving row carries an address.
  ALTER TABLE shop_registry ALTER COLUMN abstract_address SET NOT NULL;

  -- Step 5: drop the legacy shop_root column so no entry retains a path.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'shop_registry' AND column_name = 'shop_root'
  ) THEN
    ALTER TABLE shop_registry DROP COLUMN shop_root;
  END IF;
END $$;
"""

# Presence heartbeat (PDR-010 / ADR-014). The watch process that holds the
# LISTEN connection ALSO emits a liveness heartbeat into bc_presence on a
# fixed cadence. The schema is pinned exactly by scenario c4b41c39d58ee2ef:
# bc_name is the PRIMARY KEY (so the UPSERT collapses all ticks — and all
# concurrent watchers — into exactly one row per BC), last_seen_at is the
# liveness timestamp the lead classifies on, and watch_session_id is an
# informational record of which watch process most recently ticked.
_DDL_PRESENCE = """
CREATE TABLE IF NOT EXISTS bc_presence (
  bc_name         TEXT PRIMARY KEY,
  last_seen_at    TIMESTAMPTZ NOT NULL,
  watch_session_id UUID NOT NULL
);
"""


def _ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
        cur.execute(_DDL_CONSUMED_COL)
        # Order matters: widen the direction CHECK and drop the legacy
        # table-wide UNIQUE before creating the partial unique index, so a
        # pre-existing production table is migrated to the nudge-admitting
        # shape (lead-xp5f / ADR-015) without weakening inbox/outbox
        # collision detection.
        cur.execute(_DDL_DIRECTION_CHECK_MIGRATION)
        cur.execute(_DDL_PARTIAL_UNIQUE_MIGRATION)
        cur.execute(_DDL_PARTIAL_UNIQUE_INDEX)
        cur.execute(_DDL_REGISTRY)
        # lead-ikp5: build the addressing-migration SQL per-call so the
        # backfill CASE consults the env-resolved slug at operation time
        # rather than a constant frozen at import.
        cur.execute(_registry_address_migration_sql())
        cur.execute(_DDL_PRESENCE)
        cur.execute(_DDL_MESSAGE_AUDIT)
    conn.commit()


# ---------------------------------------------------------------------------
# Collision error
# ---------------------------------------------------------------------------


class CollisionError(Exception):
    """Raised when an INSERT would violate the UNIQUE constraint."""


class OutboxDepositError(Exception):
    """Raised when a postgres outbox deposit fails (Step 2 of the bd-first
    send protocol). Distinct from CollisionError: a CollisionError means the
    row already exists (idempotent / already-sent), whereas an
    OutboxDepositError means the deposit could not be performed at all
    (network drop, DB-side rejection). The send command translates this into
    a non-zero exit that names the postgres failure, and crucially leaves the
    Step-1 bd entry at dispatch_state=outbox_pending so the sweeper can
    recover it (lead-tuu5 / ADR-012)."""


# ---------------------------------------------------------------------------
# bc identifier helpers
# ---------------------------------------------------------------------------


def _bc_id(bc_root: str) -> str:
    """Return the bc identifier for an addressed shop.

    ADR-020 / PDR-007 Option A: the ``bc``/``to`` column in the messages
    table is the shop's ABSTRACT ADDRESS (``<system>/<name>``; the lead
    collapses to ``<system>/lead``), not a filesystem path. Callers thread
    the abstract address through the ``bc_root`` parameter (the name is
    retained for continuity with the pre-ADR-020 plumbing); this function is
    the identity projection of that address onto the column value.
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
    # lead-tuu5 / ADR-012 atomicity test seam: when SHOPMSG_FAIL_NEXT_OUTBOX_INSERT
    # is set (to any non-empty value), the next Step-2 dispatch deposit raises an
    # OutboxDepositError to simulate a postgres network drop or DB-side
    # rejection between Steps 1 and 3 of the bd-first send protocol. The
    # seam is consumed (the env var is cleared) so exactly one insert fails,
    # mirroring a transient outage. Step 2 of `shop-msg send` is the lead->BC
    # dispatch deposit, which lands as a direction='inbox' row with
    # allow_multi_type=False; the seam fires precisely there so the bd-first
    # Step 1 (a bd write, not a postgres write) is unaffected and the simulated
    # failure lands at Step 2.
    if (
        direction == "inbox"
        and not allow_multi_type
        and os.environ.get("SHOPMSG_FAIL_NEXT_OUTBOX_INSERT")
    ):
        del os.environ["SHOPMSG_FAIL_NEXT_OUTBOX_INSERT"]
        raise OutboxDepositError(
            f"postgres outbox insert failed (simulated): bc={bc_root!r} "
            f"work_id={work_id!r} message_type={message_type!r}"
        )
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

            # ON CONFLICT targets the PARTIAL unique index (scoped to
            # direction IN ('inbox','outbox')); the index predicate must be
            # restated in the conflict target for postgres to infer it.
            # insert_message is only ever called with inbox/outbox directions
            # (nudges go through insert_nudge), so every row this statement
            # inserts is covered by the partial index — collision detection is
            # byte-for-byte the same as the prior table-wide UNIQUE.
            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (bc, work_id, direction, message_type)
                  WHERE direction IN ('inbox','outbox') DO NOTHING
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


def insert_nudge(
    recipient_root: str,
    work_id: str | None,
    payload: dict[str, Any],
) -> int:
    """Insert a direction='nudge' row and return its row id.

    A nudge is auxiliary signaling (ADR-015 decision 6 / lead-xp5f): it is
    stored at direction='nudge', which is OUTSIDE the partial unique index
    that enforces inbox/outbox collision. As a result a SECOND (or Nth) nudge
    against the same (recipient, work_id) is storable — never raises
    CollisionError. The discriminator distinguishing multiple nudges is the
    BIGSERIAL ``id`` (returned here) plus ``created_at``.

    The ``recipient_root`` is the path of the shop the nudge is addressed TO
    (the lead for a BC->lead nudge, the BC for a lead->BC nudge); it becomes
    the ``bc`` column value, consistent with how dispatch rows key on the
    recipient's path. ``work_id`` may be None for a bare liveness nudge that
    references no in-flight dispatch; it is stored as the empty string so the
    NOT NULL column is satisfied while remaining distinguishable.

    This function NEVER touches inbox/outbox rows: the original
    direction='inbox' dispatch row for the same (recipient, work_id) is left
    byte-identical (lead-xp5f decision 1 invariant (c)).
    """
    bc = _bc_id(recipient_root)
    payload_json = json.dumps(payload)
    work_id_col = work_id if work_id is not None else ""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, 'nudge', 'nudge', %s::jsonb)
                RETURNING id
                """,
                (bc, work_id_col, payload_json),
            )
            row = cur.fetchone()
        conn.commit()
    # row is a dict (dict_row factory); the RETURNING column is "id".
    return int(row["id"]) if isinstance(row, dict) else int(row[0])


def count_nudges(recipient_root: str, work_id: str | None = None) -> int:
    """Return the number of direction='nudge' rows for a recipient.

    When ``work_id`` is supplied, counts only nudges keyed to that work_id;
    otherwise counts every nudge addressed to ``recipient_root``. Used by the
    scenarios to assert multi-delivery (a second nudge raises the count to 2)
    and rejection (a rejected send leaves the count at 0).
    """
    bc = _bc_id(recipient_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            if work_id is None:
                cur.execute(
                    "SELECT count(*) AS n FROM messages "
                    "WHERE bc = %s AND direction = 'nudge'",
                    (bc,),
                )
            else:
                cur.execute(
                    "SELECT count(*) AS n FROM messages "
                    "WHERE bc = %s AND direction = 'nudge' AND work_id = %s",
                    (bc, work_id),
                )
            row = cur.fetchone()
    return int(row["n"]) if isinstance(row, dict) else int(row[0])


def read_nudge_rows(recipient_root: str, work_id: str | None = None) -> list[dict[str, Any]]:
    """Return all direction='nudge' rows for a recipient, oldest-first.

    Each row dict carries id, work_id, message_type, payload, created_at.
    Used by step defs to assert the stored reason / note / work_id and to
    distinguish a second nudge from the first by its id discriminator.
    """
    bc = _bc_id(recipient_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            if work_id is None:
                cur.execute(
                    "SELECT id, work_id, message_type, payload, created_at "
                    "FROM messages WHERE bc = %s AND direction = 'nudge' "
                    "ORDER BY id ASC",
                    (bc,),
                )
            else:
                cur.execute(
                    "SELECT id, work_id, message_type, payload, created_at "
                    "FROM messages WHERE bc = %s AND direction = 'nudge' "
                    "AND work_id = %s ORDER BY id ASC",
                    (bc, work_id),
                )
            return list(cur.fetchall())


def query_pending_inbox(bc_root: str) -> list[tuple[str, str]]:
    """Return (work_id, message_type) pairs for unprocessed inbox messages.

    A message is 'pending' iff EITHER:

    * its inbox row has no corresponding outbox row for the same
      (bc, work_id) — the original, never-responded-to dispatch; OR

    * it is a ``clarify_response`` inbox row that is NEWER than every outbox
      row for that (bc, work_id) — i.e. the lead has answered the BC's
      outstanding clarify IN BAND, RE-OPENING the dispatch on the SAME work_id
      for the BC's gated loop to resume (lead-ox8).

    The second clause is what makes ``shop-msg send clarify_response`` re-open a
    dispatch the BC had already responded to with a ``clarify`` (which left a
    BC-side ``direction='outbox', message_type='clarify'`` marker that the first
    clause's ``NOT EXISTS (outbox)`` would otherwise suppress). The
    clarify_response row carries the lead's answer; surfacing it here is how the
    resolution becomes readable by the BC via ``shop-msg pending inbox``. The
    clause is scoped to ``clarify_response`` precisely so a stale dispatch the BC
    legitimately closed out does NOT spuriously re-open — only an in-band answer
    that post-dates the BC's response re-opens it.
    """
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.work_id, i.message_type
                FROM messages i
                WHERE i.bc = %s AND i.direction = 'inbox'
                  AND i.consumed = FALSE
                  AND (
                    NOT EXISTS (
                      SELECT 1 FROM messages o
                      WHERE o.bc = i.bc
                        AND o.work_id = i.work_id
                        AND o.direction = 'outbox'
                    )
                    OR (
                      i.message_type = 'clarify_response'
                      AND i.created_at > (
                        SELECT max(o2.created_at) FROM messages o2
                        WHERE o2.bc = i.bc
                          AND o2.work_id = i.work_id
                          AND o2.direction = 'outbox'
                      )
                    )
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
    outbox row exists (the lead has not yet consumed/acted on it).

    ADR-020: the ``bc`` column is the BC's abstract address
    (``<system>/<name>``), not a filesystem path. The ``bc_name`` projected
    back is the abstract address's trailing component (the BC's name part).
    The pre-ADR-020 ``<lead_root>/repos/`` path-prefix restriction is gone
    (it has no meaning once the registry stores no path); the lead-side
    enumeration spans every BC abstract address with an outbox row.

    When ``bc_filter`` is given it is the BC's name part; restrict to rows
    whose abstract address trailing component equals it.
    """
    import os as _os

    with _connect() as conn:
        with conn.cursor() as cur:
            if bc_filter is not None:
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
    for row in rows:
        bc = row["bc"]
        bc_name = _os.path.basename(bc)
        # A defensive guard: when a filter is supplied, the trailing component
        # must match it exactly (the LIKE above also matches longer suffixes
        # sharing the same tail, which we exclude here).
        if bc_filter is not None and bc_name != bc_filter:
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


def dispatch_inbox_row_exists(bc_root: str, work_id: str, message_type: str) -> bool:
    """Return True iff the dispatch row a ``shop-msg send`` deposits exists.

    A lead dispatch lands in the BC's table as a ``direction='inbox'`` row
    (the BC reads it from its inbox). The sweeper's reconciliation question —
    "did Step 2 land for this dispatch?" — must therefore check the inbox row
    keyed by (bc, work_id, message_type), NOT a ``direction='outbox'`` row
    (which is a BC RESPONSE, a different message). This is the authoritative
    "was the message sent" check per PDR-010 decision 3.
    """
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM messages
                WHERE bc = %s AND work_id = %s
                  AND direction = 'inbox' AND message_type = %s
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


def mark_bc_inbox_consumed(bc_root: str, work_id: str) -> bool:
    """Mark the BC's OWN inbox dispatch row(s) consumed=TRUE.

    A BC consumes an inbox dispatch once it has taken the work on; the
    consumed marker is what drops the row from ``shop-msg pending inbox --bc``
    (the BC-side counterpart to the lead-inbox consume). ``retract inbox``
    distinguishes this consumed state — a consumed deposit is REFUSED
    retraction, whereas a still-pending one is removed. Scoped to
    (bc, work_id, direction='inbox'); marks every matching unconsumed row.
    Returns True iff at least one row was marked.
    """
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages
                SET consumed = TRUE
                WHERE bc = %s AND work_id = %s
                  AND direction = 'inbox'
                  AND consumed = FALSE
                """,
                (bc, work_id),
            )
            rows_affected = cur.rowcount
        conn.commit()
    return rows_affected > 0


def _record_audit_event(
    cur, bc: str, work_id: str, message_type: str, event: str
) -> None:
    """Append one row to the messaging audit trail (within an open cursor).

    Called inside the same transaction as the action being audited so the
    record and the action commit atomically.
    """
    cur.execute(
        """
        INSERT INTO message_audit (bc, work_id, message_type, event)
        VALUES (%s, %s, %s, %s)
        """,
        (bc, work_id, message_type, event),
    )


def retract_inbox_message(bc_root: str, work_id: str, message_type: str) -> str:
    """Retract a still-pending inbox dispatch, or refuse a consumed one.

    Semantics (lead-9xrd):

    * **Still pending** — an inbox row exists for the (bc, work_id,
      message_type) triple and is NOT consumed: DELETE it so it is absent
      from ``pending inbox`` AND ``read inbox`` reports not-found. Record
      event='retracted' in the audit trail. Returns ``"retracted"``.

    * **Consumed** — the inbox row exists but is consumed=TRUE (the BC
      already took the work on): REFUSE. The deposit is left INTACT (no
      delete, no marker change). Record event='retract_refused'. Returns
      ``"refused"``.

    * **Absent / already-retracted** — no inbox row for the triple: a no-op
      SUCCESS (idempotence). No audit record is written (there is no action
      to record). Returns ``"absent"``.

    The audit write shares the transaction with the delete (or the
    no-op), so a successful retraction and its audit record commit
    atomically.
    """
    bc = _bc_id(bc_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT consumed FROM messages
                WHERE bc = %s AND work_id = %s
                  AND direction = 'inbox' AND message_type = %s
                LIMIT 1
                """,
                (bc, work_id, message_type),
            )
            row = cur.fetchone()
            if row is None:
                # Absent / already-retracted: idempotent no-op success.
                conn.commit()
                return "absent"
            if row["consumed"]:
                # Consumed deposit: refuse, leave it intact, record the
                # refused attempt.
                _record_audit_event(cur, bc, work_id, message_type, "retract_refused")
                conn.commit()
                return "refused"
            # Still pending: remove it and record the retraction.
            cur.execute(
                """
                DELETE FROM messages
                WHERE bc = %s AND work_id = %s
                  AND direction = 'inbox' AND message_type = %s
                """,
                (bc, work_id, message_type),
            )
            _record_audit_event(cur, bc, work_id, message_type, "retracted")
        conn.commit()
    return "retracted"


def consume_outbox_message(bc_root: str, work_id: str, message_type: str) -> bool:
    """Mark a specific outbox row as consumed so it no longer appears in pending output.

    Returns True if the row was found and marked consumed, False if no matching
    unconsumed outbox row exists. Does not raise on missing rows; the CLI layer
    translates False into a non-zero exit with a descriptive error.

    Recovery-surface symmetry (lead-nn5f): consume is one of two recovery
    paths (the other being ``shop-msg respond --force``). For the two to
    compose, consume must do more than flip the BC-outbox marker — in the
    SAME transaction it also releases the lead-inbox row at
    (bc=lead_root, direction='inbox', work_id, message_type) by DELETE,
    scoped to exactly the same (bc, work_id, message_type) triple as the
    --force DELETE in :func:`insert_bc_response`. After consume the
    response is no longer authoritative, so the BC may re-emit cleanly
    under the original verb WITHOUT escalating to --force: there is no
    surviving lead-inbox row to collide against. The DELETE is triple-
    scoped, so a different message_type's row on the same work_id is left
    intact on both surfaces.
    """
    bc = _bc_id(bc_root)
    lead_root = resolve_lead_shop()
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
            # Release the lead-inbox slot in the SAME transaction, scoped to
            # the same (bc, work_id, message_type) triple as the --force
            # DELETE so the two recovery paths compose without cross-talk.
            # A no-op when no lead is registered or no matching lead-inbox
            # row exists.
            if lead_root is not None:
                lead_bc_id = _bc_id(lead_root)
                cur.execute(
                    """
                    DELETE FROM messages
                    WHERE bc = %s AND work_id = %s
                      AND direction = 'inbox' AND message_type = %s
                    """,
                    (lead_bc_id, work_id, message_type),
                )
        conn.commit()
    return rows_affected > 0


def consume_lead_inbox_message(lead_root: str, work_id: str) -> bool:
    """Mark a lead's OWN inbox row consumed so it drops from pending output.

    Lead-side counterpart to :func:`consume_outbox_message` for the inbox
    direction (lead-rcjf, scenario c4dbfe1cd31d0aea). The lead's inbox is
    keyed on the lead's abstract address with direction='inbox'; this lets a
    lead drain a consumed/superseded message from its own inbox via the CLI.

    Scoped to (bc=lead_address, work_id, direction='inbox') and marks every
    matching unconsumed row consumed (a work_id may carry more than one
    message_type in the lead inbox). Returns True if at least one unconsumed
    row was marked, False otherwise. Does not raise on missing rows; the CLI
    layer translates False into a non-zero exit with a descriptive error.
    """
    bc = _bc_id(lead_root)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE messages
                SET consumed = TRUE
                WHERE bc = %s AND work_id = %s
                  AND direction = 'inbox'
                  AND consumed = FALSE
                """,
                (bc, work_id),
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


def registry_add(name: str, shop_type: str = "bc") -> None:
    """Register a shop by canonical name (ADR-020: no filesystem path).

    The registry stores NO path. Each entry carries the canonical name, the
    abstract address projected from that name + shop_type (lead -> sentinel),
    and the shop_type. Re-registering the same name with the same shop_type
    is a no-op; a shop_type change updates the entry (and its derived
    abstract address) via upsert semantics.
    """
    abstract = _abstract_address_for(name, shop_type)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shop_registry (name, abstract_address, shop_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                  SET abstract_address = EXCLUDED.abstract_address,
                      shop_type = EXCLUDED.shop_type
                """,
                (name, abstract, shop_type),
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
    """Return all registry entries as (name, abstract_address, shop_type) triples.

    ADR-020: there is no shop_root projection — the registry exposes the
    abstract address only, never a filesystem path.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, abstract_address, shop_type
                FROM shop_registry
                ORDER BY name
                """
            )
            return [
                (row["name"], row["abstract_address"], row["shop_type"])
                for row in cur.fetchall()
            ]


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
            # Upsert all BCs from manifest. ADR-020: the manifest's path
            # values are ignored — the registry stores only the canonical
            # name, its abstract address, and the shop_type.
            for name in manifest_bcs:
                cur.execute(
                    """
                    INSERT INTO shop_registry (name, abstract_address, shop_type)
                    VALUES (%s, %s, 'bc')
                    ON CONFLICT (name) DO UPDATE
                      SET abstract_address = EXCLUDED.abstract_address,
                          shop_type = 'bc'
                    """,
                    (name, _abstract_address_for(name, "bc")),
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


def resolve_root_to_name(abstract_address: str) -> str | None:
    """Reverse-resolve an abstract address to its canonical registered name.

    ADR-020: the messages column keys on the abstract address, so the
    presence heartbeat reverse-lookup maps an abstract address (e.g.
    ``shopsystem/messaging``) back to the canonical name
    (``shopsystem-messaging``). Returns None if no entry carries that
    address.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM shop_registry WHERE abstract_address = %s LIMIT 1",
                (abstract_address,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return row["name"]


def resolve_shop_name(name: str) -> str | None:
    """Resolve a canonical shop name to its abstract address.

    ADR-020: the registry stores no filesystem path; resolution yields the
    shop's abstract address (``<system>/<name>``; the lead collapses to
    ``<system>/lead``), which is the value threaded as the messages
    ``bc``/``to`` column key. Returns None if the name is not registered.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT abstract_address FROM shop_registry WHERE name = %s",
                (name,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return row["abstract_address"]


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
    """Return the lead shop's abstract address, or None if no lead registered.

    ADR-020: all lead shops collapse to the sentinel abstract address
    ``<system>/lead``; ``shop-msg respond`` routes BC responses to that
    address (Brief-006 scope C). Returns None when no lead is registered so
    callers preserve their "no lead" diagnostics.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT abstract_address FROM shop_registry
                WHERE shop_type = 'lead'
                ORDER BY name
                LIMIT 1
                """
            )
            row = cur.fetchone()
    if row is None:
        return None
    return row["abstract_address"]


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

            # 1. Write to lead inbox. (ON CONFLICT restates the partial-index
            # predicate; both writes here are inbox/outbox so they are covered
            # by messages_inbox_outbox_uq — lead-xp5f.)
            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, 'inbox', %s, %s::jsonb)
                ON CONFLICT (bc, work_id, direction, message_type)
                  WHERE direction IN ('inbox','outbox') DO NOTHING
                """,
                (lead_bc_id, work_id, message_type, payload_json),
            )
            lead_rows = cur.rowcount

            # 2. Write BC-side marker (for pending inbox --bc tracking).
            cur.execute(
                """
                INSERT INTO messages (bc, work_id, direction, message_type, payload)
                VALUES (%s, %s, 'outbox', %s, %s::jsonb)
                ON CONFLICT (bc, work_id, direction, message_type)
                  WHERE direction IN ('inbox','outbox') DO NOTHING
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
# Presence heartbeat + classification (PDR-010 / ADR-014)
#
# The watch process that holds the LISTEN connection ALSO emits a liveness
# heartbeat into bc_presence on a fixed cadence (default 30s). The load-bearing
# property (scenario c4b41c39d58ee2ef) is that liveness is emitted by the SAME
# process holding LISTEN, so a wedged loop (LISTEN intact but tick loop stalled)
# is detectable by the staleness of last_seen_at.
#
# Classification boundaries are EXACT per ADR-014 decision 3
# (scenario 3efb5c9d29f645d9):
#   - age <  90s              -> "online"   (90 itself is NOT online)
#   - 90s <= age <  300s      -> "stale"    (300 itself is NOT stale)
#   - age >= 300s             -> "offline"
#   - no bc_presence row      -> "offline"  (fail-safe: never observed alive)
# ---------------------------------------------------------------------------

# Default heartbeat cadence in seconds. The watch loop wakes on this interval
# (or sooner, on a real NOTIFY) and UPSERTs the heartbeat.
PRESENCE_TICK_SECONDS = 30

# Classification thresholds (seconds). Boundaries are half-open per ADR-014
# decision 3: online is strictly under ONLINE_MAX; stale is [ONLINE_MAX, STALE_MAX);
# offline is at-or-beyond STALE_MAX.
PRESENCE_ONLINE_MAX_SECONDS = 90
PRESENCE_STALE_MAX_SECONDS = 300


def classify_presence_age(age_seconds: float) -> str:
    """Classify a BC by the age (seconds) of its most recent heartbeat.

    Exact boundaries per ADR-014 decision 3: <90 online, [90,300) stale,
    >=300 offline. The boundary values themselves (90, 300) fall into the
    HIGHER-staleness band (90 is stale, 300 is offline).
    """
    if age_seconds < PRESENCE_ONLINE_MAX_SECONDS:
        return "online"
    if age_seconds < PRESENCE_STALE_MAX_SECONDS:
        return "stale"
    return "offline"


def presence_upsert(
    bc_name: str,
    watch_session_id: str,
    *,
    last_seen_at: Any = None,
) -> None:
    """UPSERT a presence heartbeat row keyed on bc_name (PRIMARY KEY).

    A single watch process calls this on each cadence tick. Because bc_name is
    the PRIMARY KEY, repeated ticks (from the same process OR from concurrent
    watchers) collapse into exactly one row whose last_seen_at advances to the
    most recent tick and whose watch_session_id records the most recent ticker.

    ``last_seen_at`` defaults to the database's now() (the authoritative clock
    for liveness). Tests pass an explicit timestamp to drive the clock
    deterministically.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            if last_seen_at is None:
                cur.execute(
                    """
                    INSERT INTO bc_presence (bc_name, last_seen_at, watch_session_id)
                    VALUES (%s, now(), %s)
                    ON CONFLICT (bc_name) DO UPDATE
                      SET last_seen_at = now(),
                          watch_session_id = EXCLUDED.watch_session_id
                    """,
                    (bc_name, watch_session_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO bc_presence (bc_name, last_seen_at, watch_session_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (bc_name) DO UPDATE
                      SET last_seen_at = EXCLUDED.last_seen_at,
                          watch_session_id = EXCLUDED.watch_session_id
                    """,
                    (bc_name, last_seen_at, watch_session_id),
                )
        conn.commit()


def presence_status(bc_name: str | None = None) -> list[dict[str, Any]]:
    """Return presence classification rows.

    Each row is a dict with keys:
      bc_name            -- the BC's canonical name
      classification     -- "online" | "stale" | "offline"
      seconds_since_last_seen -- float age in seconds, or None when never seen
      last_seen_at       -- the raw timestamp, or None when never seen

    When ``bc_name`` is None, returns one row per bc_presence row (full
    topology), ordered by bc_name. When ``bc_name`` is given, returns exactly
    one row for that BC: if no bc_presence row exists, the row is synthesised
    as "offline" with no last_seen_at (fail-safe rollout-window posture per
    ADR-014 consequences — a BC never observed alive is treated as offline).

    The age is computed against the database's now() (EXTRACT EPOCH of the
    delta) so the liveness clock is the DB clock, consistent with the now()
    used by presence_upsert.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            if bc_name is None:
                cur.execute(
                    """
                    SELECT bc_name, last_seen_at,
                           EXTRACT(EPOCH FROM (now() - last_seen_at)) AS age
                    FROM bc_presence
                    ORDER BY bc_name
                    """
                )
                rows = cur.fetchall()
                result = []
                for row in rows:
                    age = float(row["age"])
                    result.append(
                        {
                            "bc_name": row["bc_name"],
                            "classification": classify_presence_age(age),
                            "seconds_since_last_seen": age,
                            "last_seen_at": row["last_seen_at"],
                        }
                    )
                return result

            cur.execute(
                """
                SELECT bc_name, last_seen_at,
                       EXTRACT(EPOCH FROM (now() - last_seen_at)) AS age
                FROM bc_presence
                WHERE bc_name = %s
                """,
                (bc_name,),
            )
            row = cur.fetchone()
    if bc_name is not None and row is None:
        # Fail-safe: a BC with no heartbeat row was never observed alive.
        return [
            {
                "bc_name": bc_name,
                "classification": "offline",
                "seconds_since_last_seen": None,
                "last_seen_at": None,
            }
        ]
    age = float(row["age"])
    return [
        {
            "bc_name": row["bc_name"],
            "classification": classify_presence_age(age),
            "seconds_since_last_seen": age,
            "last_seen_at": row["last_seen_at"],
        }
    ]


def run_presence_heartbeat(
    bc_name: str,
    watch_session_id: str,
    *,
    stop_event=None,
    max_ticks: int | None = None,
) -> None:
    """Emit presence heartbeats on the cadence until ``stop_event`` is set.

    Called by ``watch_inbox`` in a background daemon thread so that liveness is
    emitted by the SAME process that holds the LISTEN connection (the load-bearing
    property of scenario c4b41c39d58ee2ef): if the LISTEN loop or this process
    wedges, last_seen_at stops advancing and the lead's classifier surfaces the
    BC as stale/offline.

    The first heartbeat fires immediately (so a fresh watch is observable within
    the first cadence window, not after a full cadence delay); subsequent
    heartbeats fire every ``PRESENCE_TICK_SECONDS`` via the ``_sleep`` seam.
    ``watch_session_id`` is constant across ticks from this process. There is no
    backfill of missed ticks across a stall or reconnect — each tick simply
    UPSERTs last_seen_at = now(); the gap is informational (scenario
    3ff862feef699480).

    Test seams: ``stop_event`` (a threading.Event) lets the test stop the loop
    after observing ticks; ``max_ticks`` bounds the loop for in-process tests
    that drive ``_sleep`` to a no-op.
    """
    ticks = 0
    while True:
        presence_upsert(bc_name, watch_session_id)
        ticks += 1
        if max_ticks is not None and ticks >= max_ticks:
            return
        if stop_event is not None and stop_event.is_set():
            return
        _sleep(PRESENCE_TICK_SECONDS)
        if stop_event is not None and stop_event.is_set():
            return


def _start_presence_heartbeat_thread(bc_name: str):
    """Start a daemon heartbeat thread for ``bc_name``; return (thread, stop_event).

    Generates a fresh per-process session UUID. The thread is a daemon so it
    never blocks process exit; ``stop_event`` lets the caller stop it cleanly in
    a finally block.
    """
    import threading
    import uuid

    stop_event = threading.Event()
    session_id = str(uuid.uuid4())

    def _run():
        try:
            run_presence_heartbeat(bc_name, session_id, stop_event=stop_event)
        except Exception:
            # A heartbeat failure must never crash the watch process; the lead's
            # fail-safe classifier treats a missing/stale heartbeat as offline.
            pass

    thread = threading.Thread(target=_run, name=f"presence-{bc_name}", daemon=True)
    thread.start()
    return thread, stop_event


def _canonical_name_from_address(abstract_address: str) -> str | None:
    """Reconstruct the canonical shop name from its abstract address.

    Inverse of ``_abstract_address_for``: an address ``<slug>/<rest>`` projects
    back to the canonical name ``<slug>-<rest>`` (the lead's sentinel
    ``<slug>/lead`` projects to no recoverable BC name and yields None). When
    the address carries no ``/`` it is not an abstract address and we return it
    unchanged.

    This is the deterministic, registry-independent recovery the presence-key
    derivation falls back to when the registry reverse-lookup misses — it never
    strips the system prefix the way ``basename`` does.
    """
    if "/" not in abstract_address:
        return abstract_address
    slug, _, rest = abstract_address.partition("/")
    if not rest or rest == "lead":
        return None
    return f"{slug}-{rest}"


def _resolve_presence_name(bc_root: str) -> str:
    """Derive the bc_presence key the heartbeat must be written under.

    This MUST equal the name ``shop-msg bc-status --bc <name>`` queries by (the
    canonical shop name), or a live, heartbeating watch reads as a false
    offline (work_id lead-bppa: the heartbeat row is written under one key while
    bc-status looks under another, so the lookup misses and the fail-safe
    classifier reports offline despite a live watch).

    Resolution order:
      1. Registry reverse-lookup (abstract address -> canonical name).
      2. On a reverse-lookup MISS (a transient/race the false-offline defect
         surfaced under), reconstruct the canonical name from the abstract
         address (``<slug>/<rest>`` -> ``<slug>-<rest>``). This is registry
         independent and prefix-preserving.
      3. Only when neither yields a name (e.g. a non-address bc_root) fall back
         to the basename, which is correct for a bare name with no ``/``.

    The prior implementation fell straight from (1) to a ``basename`` that
    stripped the system prefix (``shopsystem/live`` -> ``live``), which is the
    root cause of the false-offline.
    """
    import os as _os

    name = resolve_root_to_name(bc_root)
    if name:
        return name
    reconstructed = _canonical_name_from_address(bc_root)
    if reconstructed:
        return reconstructed
    return _os.path.basename(bc_root.rstrip("/"))


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

    # Presence heartbeat (PDR-010 / ADR-014): liveness is emitted by the SAME
    # process that holds this LISTEN connection. We key the heartbeat on the
    # canonical BC name (registry reverse-lookup), falling back to the bc_root
    # basename when the BC is not registered. The ticker runs in a daemon
    # thread so it composes with — and never blocks — the LISTEN loop, and is
    # stopped cleanly in the finally block.
    heartbeat_thread = None
    heartbeat_stop = None
    # The heartbeat MUST be keyed on the same canonical name `shop-msg
    # bc-status --bc <name>` queries by, or a live watch reads as a false
    # offline (work_id lead-bppa). _resolve_presence_name reconstructs the
    # canonical name from the abstract address on a registry reverse-lookup
    # miss instead of falling back to a prefix-stripping basename.
    presence_name = _resolve_presence_name(bc_root)

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

        # Start the presence heartbeat thread now that the schema exists and
        # LISTEN is established (so liveness only flows once the watch is fully
        # armed). The first tick fires immediately inside the thread.
        heartbeat_thread, heartbeat_stop = _start_presence_heartbeat_thread(
            presence_name
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
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        conn.close()
