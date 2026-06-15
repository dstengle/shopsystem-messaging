"""lead-ikp5: complete the SHOPMSG_SYSTEM_SLUG externalization.

work_id lead-tgsb externalized the slug via ``_get_system_slug()`` and threaded
it through ``_abstract_address_for`` (so ``registry_add`` already projects under
the configured slug). But TWO sites stayed bound to the frozen module constant
``DEFAULT_SYSTEM_SLUG`` and so always projected ``shopsystem/...`` regardless of
``SHOPMSG_SYSTEM_SLUG``:

  1. the ``LEAD_ABSTRACT_ADDRESS`` sentinel (computed at import), and
  2. the registry addressing-migration UPDATE ... SET ... CASE, whose SQL
     string interpolated the frozen ``LEAD_ABSTRACT_ADDRESS`` / ``SYSTEM_SLUG``
     at import time.

These tests pin the remaining fix: both sites must consult the env-resolved
slug at call time, and the default (unset) projection must NOT regress.

Each test sweeps the rows it creates so it does not pollute the shared registry.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager

from shop_msg import storage


@contextmanager
def _system_slug(value: str | None):
    """Set (or clear) SHOPMSG_SYSTEM_SLUG for the duration of the block."""
    sentinel = object()
    prior = os.environ.get("SHOPMSG_SYSTEM_SLUG", sentinel)
    if value is None:
        os.environ.pop("SHOPMSG_SYSTEM_SLUG", None)
    else:
        os.environ["SHOPMSG_SYSTEM_SLUG"] = value
    try:
        yield
    finally:
        if prior is sentinel:
            os.environ.pop("SHOPMSG_SYSTEM_SLUG", None)
        else:
            os.environ["SHOPMSG_SYSTEM_SLUG"] = prior  # type: ignore[arg-type]


def _registry_address(name: str) -> str | None:
    for entry_name, address, _shop_type in storage.registry_list():
        if entry_name == name:
            return address
    return None


def _insert_null_address_row(name: str, shop_type: str) -> None:
    """Insert a shop_registry row whose abstract_address is NULL.

    This reproduces the pre-migration / legacy shape the addressing-migration
    backfill CASE is responsible for projecting. The migration's UPDATE only
    touches rows ``WHERE abstract_address IS NULL``, so this is the exact input
    that exercises the SQL CASE projection.
    """
    with storage._connect() as conn:
        with conn.cursor() as cur:
            # The column is NOT NULL after migration; drop the constraint
            # locally for the insert so we can seed a NULL-address legacy row,
            # then the migration re-establishes NOT NULL after backfilling.
            cur.execute(
                "ALTER TABLE shop_registry ALTER COLUMN abstract_address DROP NOT NULL"
            )
            cur.execute(
                """
                INSERT INTO shop_registry (name, abstract_address, shop_type)
                VALUES (%s, NULL, %s)
                ON CONFLICT (name) DO UPDATE
                  SET abstract_address = NULL, shop_type = EXCLUDED.shop_type
                """,
                (name, shop_type),
            )
        conn.commit()


def test_migration_backfill_bc_row_projects_configured_slug():
    """A legacy NULL-address BC row, backfilled by the addressing-migration CASE
    under SHOPMSG_SYSTEM_SLUG=dummyco, projects dummyco/<rest> (NOT shopsystem/).

    The wall before the fix: the migration SQL froze SYSTEM_SLUG='shopsystem' at
    import, so the CASE always projected shopsystem/<rest>."""
    suffix = uuid.uuid4().hex[:8]
    name = f"dummyco-greeter-{suffix}"
    try:
        with _system_slug("dummyco"):
            _insert_null_address_row(name, "bc")
            # _connect() runs _ensure_schema -> the addressing migration, which
            # backfills the NULL-address row via the SQL CASE.
            storage._connect().close()
            assert _registry_address(name) == f"dummyco/greeter-{suffix}"
    finally:
        storage.registry_remove(name)


def test_migration_backfill_unprefixed_bc_row_projects_configured_slug():
    """A legacy NULL-address BC row whose name does not carry the configured
    slug prefix is placed under the configured slug as dummyco/<name>."""
    suffix = uuid.uuid4().hex[:8]
    name = f"acme-greeter-{suffix}"
    try:
        with _system_slug("dummyco"):
            _insert_null_address_row(name, "bc")
            storage._connect().close()
            assert _registry_address(name) == f"dummyco/{name}"
    finally:
        storage.registry_remove(name)


def test_migration_backfill_lead_row_projects_configured_sentinel():
    """A legacy NULL-address lead row, backfilled under SHOPMSG_SYSTEM_SLUG=dummyco,
    collapses to the dummyco/lead sentinel (NOT shopsystem/lead)."""
    suffix = uuid.uuid4().hex[:8]
    name = f"dummyco-product-{suffix}"
    try:
        with _system_slug("dummyco"):
            _insert_null_address_row(name, "lead")
            storage._connect().close()
            assert _registry_address(name) == "dummyco/lead"
    finally:
        storage.registry_remove(name)


def test_lead_sentinel_resolves_per_call_under_configured_slug():
    """resolve_lead_shop / the sentinel must reflect the configured slug at call
    time, not a constant frozen at import: registering a lead under dummyco and
    resolving it yields dummyco/lead."""
    suffix = uuid.uuid4().hex[:8]
    name = f"dummyco-product-{suffix}"
    try:
        with _system_slug("dummyco"):
            storage.registry_add(name, shop_type="lead")
            assert _registry_address(name) == "dummyco/lead"
            assert storage.resolve_lead_shop() == "dummyco/lead"
    finally:
        storage.registry_remove(name)


def test_migration_backfill_default_path_non_regressing():
    """Critical default-path guard (criterion 4): with SHOPMSG_SYSTEM_SLUG unset,
    the migration backfill CASE still projects shopsystem/<rest> for a BC row and
    shopsystem/lead for a lead row. The live shopsystem fleet must be unaffected."""
    suffix = uuid.uuid4().hex[:8]
    bc_name = f"shopsystem-widget-{suffix}"
    lead_name = f"shopsystem-product-{suffix}"
    try:
        with _system_slug(None):
            _insert_null_address_row(bc_name, "bc")
            _insert_null_address_row(lead_name, "lead")
            storage._connect().close()
            assert _registry_address(bc_name) == f"shopsystem/widget-{suffix}"
            assert _registry_address(lead_name) == "shopsystem/lead"
    finally:
        storage.registry_remove(bc_name)
        storage.registry_remove(lead_name)
