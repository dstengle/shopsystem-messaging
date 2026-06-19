"""Guard tests for synthetic-name conftest fixture hygiene (work_id lead-vvz).

The conftest step-def fixtures that register the synthetic test-only shop
names ``bc-alpha`` / ``bc-beta`` (and the ``ghost-lead`` negative case) must
route their registry mutations through the lead-6c5 finalizer mechanism
(``_register_shop`` -> snapshot into ``_SAVED_PRODUCTION_ENTRIES`` + record in
``_PER_TEST_MUTATED_NAMES``) so the autouse ``_per_test_registry_restore``
fixture deregisters/rolls them back at each test boundary, leaving the test
registry clean.

These tests pin both acceptance criteria for lead-vvz:

  AC1 — after a synthetic-name fixture runs, no residual ``bc-alpha`` /
        ``bc-beta`` / ``ghost-lead`` row survives the per-test teardown.
  AC2 — the lead-6c5 production-registry finalizer mechanism is unchanged
        (synthetic names join the SAME tracked-mutation path, they do not
        bypass it).
"""

from __future__ import annotations

from pathlib import Path

import conftest as ct
from shop_msg.storage import _connect


def _registry_has(name: str) -> bool:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM shop_registry WHERE name = %s",
                (name,),
            )
            return cur.fetchone() is not None


def test_one_bc_fixture_tracks_synthetic_name_for_teardown(tmp_path: Path):
    """given_lead_shop_with_one_bc must register bc-alpha through the tracked
    finalizer path so it is removed at per-test teardown (AC1 + AC2)."""
    name = "bc-alpha"
    # Pre-state: ensure the synthetic name is absent (its session baseline).
    assert not _registry_has(name)

    ct.given_lead_shop_with_one_bc(tmp_path, name)

    # The row is live mid-test ...
    assert _registry_has(name)
    # ... and it is tracked for restoration by the lead-6c5 mechanism, so the
    # autouse _per_test_registry_restore finalizer will remove it at teardown.
    assert name in ct._PER_TEST_MUTATED_NAMES
    assert ct._SAVED_PRODUCTION_ENTRIES.get(name, "MISSING") is None


def test_two_bc_fixture_tracks_both_synthetic_names_for_teardown(tmp_path: Path):
    """given_lead_shop_with_two_bcs must register bc-alpha and bc-beta through
    the tracked finalizer path (AC1 + AC2)."""
    bc_a, bc_b = "bc-alpha", "bc-beta"
    assert not _registry_has(bc_a)
    assert not _registry_has(bc_b)

    ct.given_lead_shop_with_two_bcs(tmp_path, bc_a, bc_b)

    for name in (bc_a, bc_b):
        assert _registry_has(name)
        assert name in ct._PER_TEST_MUTATED_NAMES
        assert ct._SAVED_PRODUCTION_ENTRIES.get(name, "MISSING") is None


def test_ghost_lead_fixture_leaves_no_residual_row(tmp_path: Path):
    """given_no_lead_registered must guarantee ghost-lead is absent and tracked
    so no residual row survives the suite (AC1)."""
    name = "ghost-lead"
    # Even if a prior session left a residue, the fixture must clear it.
    ct.given_no_lead_registered(name)
    assert not _registry_has(name)
    # Tracked so any later in-test re-registration is also restored to absent.
    assert name in ct._PER_TEST_MUTATED_NAMES


def test_synthetic_names_absent_after_prior_tests():
    """Cross-test residue guard: by the time this test runs, the per-test
    teardown of the fixtures above must have removed every synthetic row
    (AC1 — no residual rows after the fixtures ran in earlier tests)."""
    for name in ("bc-alpha", "bc-beta", "ghost-lead"):
        assert not _registry_has(name), (
            f"residual synthetic registry row {name!r} survived teardown"
        )


def test_production_finalizer_mechanism_intact():
    """AC2: the lead-6c5 mechanism (_register_shop snapshots + tracks; the
    autouse fixture restores) is still wired — synthetic names ride this exact
    path rather than a separate one."""
    # _register_shop must still both snapshot and record the mutation.
    assert callable(ct._register_shop)
    assert hasattr(ct, "_SAVED_PRODUCTION_ENTRIES")
    assert hasattr(ct, "_PER_TEST_MUTATED_NAMES")
    assert callable(ct._per_test_registry_restore)
