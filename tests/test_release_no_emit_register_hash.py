"""Register-hash guard for the no-emit release-workflow scenario (work_id
lead-0udp — the authoritative correction of lead-n8pf/lead-qz1w).

lead-n8pf embedded the dispatch's provenance as `#` comment lines INSIDE the
gherkin scenario block. ADR-019 hashes comment lines as part of the
scenario-block-only canonicalization, so the contaminated on-disk block
hashed to the WRONG value (974ee23c53cbb09a). The lead's correct register
value for the CLEAN block (the `@scenario_hash:` tag line, the `Scenario:`
line, and its Given/When/Then/And steps, with NO `#` comment lines) is
fd28deb48a7c75f4 — for register parity with shopsystem-scenarios.

This test pins TWO facts about the on-disk feature file:

 1. The on-disk `@scenario_hash:` tag reads exactly `fd28deb48a7c75f4`.
 2. The on-disk scenario block, canonicalized block-only via the BC's
    canonical `scenarios hash` CLI (ADR-019, scenario 117), RECOMPUTES to
    exactly that same value — i.e. the on-disk tag is not fabricated or
    stale, and no comment contamination remains inside the block.

If the block still carried `#` comment contamination (or any body
divergence), fact (2) would recompute to a value other than the on-disk tag
and this test would fail. That is the recompute-equality teeth.
"""
import subprocess
from pathlib import Path

_REGISTER_HASH = "fd28deb48a7c75f4"
_WRONG_CONTAMINATED_HASH = "974ee23c53cbb09a"
_FEATURE = (
    Path(__file__).resolve().parent.parent
    / "features"
    / "release_workflow_no_bc_launcher_dispatch_emit.feature"
)


def _scenario_block(feature_text: str) -> str:
    """Extract the single scenario block AS STORED ON DISK: every line from the
    `@scenario_hash:` tag line through the last step, VERBATIM.

    Crucially this does NOT strip `#` comment lines. The canonical block-only
    hasher (ADR-019) keeps every non-blank, non-`@scenario_hash:` line —
    including any `#` comment line — so feeding it the block exactly as the
    file stores it is what makes recompute-equality detect comment
    contamination: a contaminated block recomputes to a DIFFERENT value than
    the clean register hash, and this test fails. Stripping comments here
    would defeat that detection.
    """
    lines = feature_text.splitlines()
    # The block starts at the @scenario_hash tag line.
    start = next(
        i for i, ln in enumerate(lines) if ln.lstrip().startswith("@scenario_hash:")
    )
    block: list[str] = []
    for ln in lines[start:]:
        block.append(ln)
    # Trim trailing blank lines.
    while block and not block[-1].strip():
        block.pop()
    return "\n".join(block) + "\n"


def test_on_disk_tag_is_the_register_hash() -> None:
    text = _FEATURE.read_text()
    assert f"@scenario_hash:{_REGISTER_HASH}" in text, (
        f"on-disk @scenario_hash tag must read {_REGISTER_HASH}; "
        f"feature file:\n{text}"
    )
    assert _WRONG_CONTAMINATED_HASH not in text, (
        f"the contaminated hash {_WRONG_CONTAMINATED_HASH} must be GONE from "
        f"the feature file (comment-block contamination removed)"
    )


def test_on_disk_block_recomputes_equal_to_register_hash() -> None:
    block = _scenario_block(_FEATURE.read_text())
    # The block must contain NO `#` comment lines (contamination check).
    for ln in block.splitlines():
        assert not ln.strip().startswith("#"), (
            f"scenario block still carries a comment line: {ln!r}"
        )
    proc = subprocess.run(
        ["scenarios", "hash"],
        input=block,
        capture_output=True,
        text=True,
        check=True,
    )
    recomputed = proc.stdout.strip()
    assert recomputed == _REGISTER_HASH, (
        f"on-disk scenario block recomputes to {recomputed}, not the "
        f"register value {_REGISTER_HASH}; block was:\n{block}"
    )


def test_scenarios_verify_exits_zero_against_register_hash() -> None:
    block = _scenario_block(_FEATURE.read_text())
    proc = subprocess.run(
        ["scenarios", "verify", "--hash", _REGISTER_HASH],
        input=block,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        f"scenarios verify --hash {_REGISTER_HASH} exited "
        f"{proc.returncode}; stderr:\n{proc.stderr}"
    )
