"""ADR-020 system-slug externalization (work_id lead-tgsb).

The abstract-address projection binds the ``<system>`` segment of every
registry entry. Before this fix the slug was a hard module constant
(``SYSTEM_SLUG = "shopsystem"``) with no input surface: a second product's
BC always deposited under ``shopsystem/<name>`` — a silent cross-product
routing defeat (PDR-018 gate condition #2). These tests pin the fix:

  * the system slug is derived from a documented configuration surface
    following the established ``SHOPMSG_DSN`` pattern — the
    ``SHOPMSG_SYSTEM_SLUG`` environment override (precedence (c)); and
  * the DEFAULT projection (slug ``shopsystem`` / unset) continues to hold,
    so scenarios 50/53 are not contradicted.

Each test sweeps the rows it creates so it does not pollute the shared
registry across runs.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager

import pytest

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


def test_env_override_projects_under_dummy_slug():
    """SHOPMSG_SYSTEM_SLUG=dummyco => registry add acme-widget projects
    dummyco/acme-widget (the wall: the env override was ignored)."""
    name = f"acme-widget-{uuid.uuid4().hex[:8]}"
    try:
        with _system_slug("dummyco"):
            storage.registry_add(name, shop_type="bc")
            assert _registry_address(name) == f"dummyco/{name}"
    finally:
        storage.registry_remove(name)


def test_env_override_strips_matching_system_prefix():
    """A name carrying the configured slug as its prefix projects with the
    prefix collapsed: slug dummyco + name dummyco-foo => dummyco/foo."""
    suffix = uuid.uuid4().hex[:8]
    name = f"dummyco-foo-{suffix}"
    try:
        with _system_slug("dummyco"):
            storage.registry_add(name, shop_type="bc")
            assert _registry_address(name) == f"dummyco/foo-{suffix}"
    finally:
        storage.registry_remove(name)


def test_default_slug_unset_still_projects_shopsystem():
    """Regression for scenarios 50/53: with the slug unset the default
    projection shopsystem/<rest> MUST continue to hold."""
    suffix = uuid.uuid4().hex[:8]
    name = f"shopsystem-widget-{suffix}"
    try:
        with _system_slug(None):
            storage.registry_add(name, shop_type="bc")
            assert _registry_address(name) == f"shopsystem/widget-{suffix}"
    finally:
        storage.registry_remove(name)


def test_lead_sentinel_follows_configured_slug():
    """The lead collapses to <slug>/lead under the configured slug, and to
    shopsystem/lead by default."""
    with _system_slug("dummyco"):
        assert storage._abstract_address_for("any-lead-name", "lead") == "dummyco/lead"
    with _system_slug(None):
        assert storage._abstract_address_for("any-lead-name", "lead") == "shopsystem/lead"


def test_routing_layer_resolves_to_dummy_address():
    """The send/pending/read CLI paths route by resolving the BC name to its
    stored abstract address (resolve_shop_name). With the dummy slug
    configured at registry-add time, that resolution yields dummyco/<name>,
    so every name-addressed operation routes there."""
    suffix = uuid.uuid4().hex[:8]
    bc_name = f"acme-widget-{suffix}"
    try:
        with _system_slug("dummyco"):
            storage.registry_add(bc_name, shop_type="bc")
            # This is exactly the value shop-msg send/pending/read route on.
            assert storage.resolve_shop_name(bc_name) == f"dummyco/{bc_name}"
    finally:
        storage.registry_remove(bc_name)


def test_cli_registry_add_projects_dummy_slug_end_to_end():
    """Acceptance pin (PDR-018 gate #2), full CLI surface: with the dummy
    slug configured via the documented env knob and no hand-edits, the
    `shop-msg registry add <name>` CLI projects dummyco/<name> and
    `shop-msg registry list` shows it."""
    import subprocess
    import sys

    suffix = uuid.uuid4().hex[:8]
    bc_name = f"acme-widget-{suffix}"
    env = dict(os.environ)
    env["SHOPMSG_SYSTEM_SLUG"] = "dummyco"
    try:
        add = subprocess.run(
            [sys.executable, "-m", "shop_msg", "registry", "add", bc_name],
            capture_output=True,
            text=True,
            env=env,
        )
        assert add.returncode == 0, add.stderr
        listing = subprocess.run(
            [sys.executable, "-m", "shop_msg", "registry", "list"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert listing.returncode == 0, listing.stderr
        line = [ln for ln in listing.stdout.splitlines() if bc_name in ln]
        assert line, f"no registry line for {bc_name}:\n{listing.stdout}"
        assert f"dummyco/{bc_name}" in line[0], line[0]
        assert f"shopsystem/{bc_name}" not in line[0], line[0]
    finally:
        storage.registry_remove(bc_name)
