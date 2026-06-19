"""Import-guard against stale non-editable first-party installs (lead-ym8f / c1f).

Root cause pinned by bd shopsystem-messaging-c1f: a frozen, NON-editable copy
of ``catalog`` (and ``shop_msg``) under ``site-packages`` can SHADOW
``/workspace/src``. When that happens, ``import catalog`` resolves to the stale
copy, pytest validates against drifted code, and correct work in ``src/`` is
masked as a false regression (e.g. ``test_work_id_pattern_symmetry`` failing
32/56 with "DID NOT RAISE ValidationError" on a clean checkout while ``src`` is
in fact correct).

The remediation is a COLLECTION-TIME guard in ``tests/conftest.py`` that asserts
every first-party package the suite imports resolves UNDER ``src/``; if a
site-packages copy shadows ``src/`` the guard fails fast — before any test runs
— naming the offending package and its resolved ``__file__`` vs the expected
``src/`` path.

These tests exercise the guard's detection logic directly. The guard's own
helper, ``_assert_first_party_under_src``, takes the set of package files to
check so a shadow can be SIMULATED (a __file__ that does not live under src/)
without mutating the live interpreter's import state and without leaving any
stale copy on disk after the test.
"""
from pathlib import Path

import pytest

from tests.conftest import (
    FIRST_PARTY_PACKAGES,
    SRC_ROOT,
    GuardError,
    _assert_first_party_under_src,
)


def test_guard_passes_when_all_first_party_resolve_under_src():
    """A clean editable install: every first-party __file__ lives under src/."""
    resolved = {
        pkg: str(SRC_ROOT / pkg / "__init__.py") for pkg in FIRST_PARTY_PACKAGES
    }
    # Must not raise.
    _assert_first_party_under_src(resolved)


def test_guard_fails_fast_when_site_packages_copy_shadows_src():
    """A frozen site-packages copy shadowing src/ must trip the guard.

    Simulate the exact c1f failure: ``catalog`` resolved from
    ``/usr/local/lib/python3.11/site-packages/catalog`` instead of src/.
    """
    stale = "/usr/local/lib/python3.11/site-packages/catalog/__init__.py"
    resolved = {
        pkg: str(SRC_ROOT / pkg / "__init__.py") for pkg in FIRST_PARTY_PACKAGES
    }
    resolved["catalog"] = stale

    with pytest.raises(GuardError) as excinfo:
        _assert_first_party_under_src(resolved)

    msg = str(excinfo.value)
    # Names the offending package, its resolved __file__, and the expected src.
    assert "catalog" in msg
    assert stale in msg
    assert str(SRC_ROOT) in msg


def test_guard_message_names_every_shadowed_package():
    """When more than one first-party package is shadowed, all are named."""
    resolved = {
        pkg: f"/usr/local/lib/python3.11/site-packages/{pkg}/__init__.py"
        for pkg in FIRST_PARTY_PACKAGES
    }
    with pytest.raises(GuardError) as excinfo:
        _assert_first_party_under_src(resolved)
    msg = str(excinfo.value)
    for pkg in FIRST_PARTY_PACKAGES:
        assert pkg in msg


def test_guard_live_imports_resolve_under_src():
    """End-to-end: the REAL live imports must resolve under src/ right now.

    This is the guard running against the actual interpreter the suite uses
    (the editable install). If this ever fails, the suite is running against a
    stale shadow and every other result is suspect.
    """
    import catalog
    import shop_msg

    for pkg, mod in {"catalog": catalog, "shop_msg": shop_msg}.items():
        resolved = Path(mod.__file__).resolve()
        assert SRC_ROOT in resolved.parents, (
            f"{pkg} resolved to {resolved}, not under {SRC_ROOT}"
        )
