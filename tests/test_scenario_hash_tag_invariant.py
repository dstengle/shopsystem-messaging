"""Suite-wide invariant: every committed ``@scenario_hash`` tag in
``features/`` equals the canonical block-only hash of its OWN scenario body.

Surfaced by lead-wek9 (a mechanism_observation during lead-xc0d
reconciliation): the stored ``@scenario_hash`` tags had drifted from the
canonical block-only hash of their bodies, so a BC that reconciled by
grepping the stored tag — instead of recomputing canonical — would MISS
scenarios the lead names by canonical hash. This invariant makes future
drift FAIL CI rather than hiding until the next reconciliation.

The canonical body of a scenario block is defined by the authoritative
producer ``shop_msg.cli._build_scenario_payload`` (ADR-019 / lead-pw41,
scenario-block-only canonical form): the ``Scenario:`` / ``Scenario
Outline:`` keyword line through the block's last step / ``Examples`` line,
with NO ``Feature:`` header line and NO tag lines. ``@scenario_hash:``
lines are dropped by ``compute_scenario_hash`` itself; the standalone
``@bc:`` tag line never participates in the canonical body, so this test
excludes EVERY ``@``-prefixed tag line from the body it hashes.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from scenarios.hash import compute_scenario_hash

_FEATURES_DIR = Path(__file__).resolve().parent.parent / "features"

_SCENARIO_RE = re.compile(r"^\s*Scenario(?:\s+Outline)?:")
_TAG_LINE_RE = re.compile(r"^\s*@")
_HASH_TAG_RE = re.compile(r"@scenario_hash:(?P<hash>\S+)")


def _iter_committed_scenario_blocks():
    """Yield ``(feature_name, title, stored_hash, canonical_hash)`` for every
    scenario in ``features/`` that carries a ``@scenario_hash`` tag.

    A scenario's tag-group is the contiguous run of tag lines immediately
    preceding its ``Scenario:`` keyword. Its body is the keyword line
    through the line just before the next scenario's tag-group (or EOF),
    with all ``@``-prefixed tag lines excluded — matching what
    ``shop_msg.cli._build_scenario_payload`` hashes.
    """
    for feature in sorted(_FEATURES_DIR.glob("*.feature")):
        lines = feature.read_text(encoding="utf-8").splitlines()
        scenario_idxs = [i for i, ln in enumerate(lines) if _SCENARIO_RE.match(ln)]
        for n, sidx in enumerate(scenario_idxs):
            # tag-group preceding this scenario
            tstart = sidx
            j = sidx - 1
            while j >= 0 and _TAG_LINE_RE.match(lines[j]):
                tstart = j
                j -= 1
            tag_lines = lines[tstart:sidx]
            stored = None
            for tl in tag_lines:
                m = _HASH_TAG_RE.search(tl)
                if m and tl.strip().startswith("@"):
                    stored = m.group("hash")
                    break
            if stored is None:
                continue
            # body ends just before the next scenario's tag-group (or EOF)
            if n + 1 < len(scenario_idxs):
                nxt = scenario_idxs[n + 1]
                k = nxt - 1
                nstart = nxt
                while k >= 0 and _TAG_LINE_RE.match(lines[k]):
                    nstart = k
                    k -= 1
                bend = nstart
            else:
                bend = len(lines)
            body_lines = [
                ln for ln in lines[sidx:bend] if not ln.strip().startswith("@")
            ]
            canonical = compute_scenario_hash("\n".join(body_lines))
            title = lines[sidx].strip()
            yield (feature.name, title, stored, canonical)


_BLOCKS = list(_iter_committed_scenario_blocks())


def test_features_dir_has_committed_scenario_hash_tags() -> None:
    """Guard against the invariant silently covering nothing: the BC's
    ``features/`` must carry committed ``@scenario_hash`` tags for the
    per-scenario invariant below to mean anything."""
    assert _BLOCKS, (
        "no committed @scenario_hash tags found under features/; the "
        "drift invariant would vacuously pass"
    )


@pytest.mark.parametrize(
    "feature_name,title,stored,canonical",
    _BLOCKS,
    ids=[f"{b[0]}::{b[1]}" for b in _BLOCKS],
)
def test_committed_scenario_hash_tag_equals_block_only_canonical(
    feature_name: str, title: str, stored: str, canonical: str
) -> None:
    """Every committed ``@scenario_hash`` tag must equal the block-only
    canonical hash of its own scenario body. A mismatch is drift: the stored
    tag no longer pins the scenario whose body it sits on."""
    assert stored == canonical, (
        f"@scenario_hash drift in {feature_name}: "
        f"{title!r} stored tag {stored} != block-only canonical "
        f"hash {canonical} of its own body. Regenerate the tag to "
        f"{canonical} (body unchanged)."
    )
