"""Teeth proof for the @scenario_hash:fd28deb48a7c75f4 no-emit register
scenario (work_id lead-0udp).

A "behavior already true" formalization is worthless if its scenario would
pass even when the emit is reintroduced. These tests prove the
comment-stripped workflows-tree inspection behind the scenario has teeth:

 1. It FAILS when a release.yml carrying the old `repository_dispatch` /
    `BC_LAUNCHER_DISPATCH_TOKEN` emit is present in the executable body.
 2. It PASSES when that file is removed (absence satisfies the guarantee).
 3. The comment-exclusion clause has teeth: a token present ONLY inside a
    YAML comment does NOT fail the guarantee.
 4. The empty / absent workflows tree satisfies the guarantee.
"""
from pathlib import Path

from tests.release_emit_inspect import (
    forbidden_tokens_in_executable_body,
    no_emit_guarantee_holds,
)

# The old emit, faithfully reproducing the retired wiring in an executable
# (non-comment) body.
_OLD_EMIT_RELEASE_YML = """\
name: release
on:
  push:
    tags:
      - "v*"
jobs:
  dispatch-bc-launcher-build:
    runs-on: ubuntu-latest
    steps:
      - name: Fan out to bc-launcher
        uses: peter-evans/repository-dispatch@v3
        with:
          token: ${{ secrets.BC_LAUNCHER_DISPATCH_TOKEN }}
          repository: dstengle/shopsystem-bc-launcher
          event-type: framework-utility-release
      - name: Fallback curl repository_dispatch
        run: |
          curl -X POST \\
            -H "Authorization: token ${{ secrets.BC_LAUNCHER_DISPATCH_TOKEN }}" \\
            https://api.github.com/repos/dstengle/shopsystem-bc-launcher/dispatches \\
            -d '{"event_type": "framework-utility-release"}'
"""

# The exact same wiring, but appearing ONLY inside YAML comments. The
# executable body is inert.
_COMMENT_ONLY_RELEASE_YML = """\
name: release
on:
  push:
    tags:
      - "v*"
# Historical note (ADR-022): this workflow used to define a
# dispatch-bc-launcher-build job performing a repository_dispatch
# targeting dstengle/shopsystem-bc-launcher with event_type
# framework-utility-release, authenticated with secrets.BC_LAUNCHER_DISPATCH_TOKEN.
# That fan-in is retired; the centralized bc-launcher poll supersedes it.
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo "version-tag release; no cross-repo emit"
"""


def _write_workflow(tmp_path: Path, content: str) -> Path:
    wf_dir = tmp_path / ".github" / "workflows"
    wf_dir.mkdir(parents=True, exist_ok=True)
    (wf_dir / "release.yml").write_text(content)
    return wf_dir


def test_scenario_fails_when_emit_present_in_executable_body(tmp_path: Path) -> None:
    wf_dir = _write_workflow(tmp_path, _OLD_EMIT_RELEASE_YML)
    violations = forbidden_tokens_in_executable_body(wf_dir)
    tokens = {tok for _, tok in violations}
    # Every facet of the retired emit must be detected in the executable body.
    assert "dispatch-bc-launcher-build" in tokens
    assert "dstengle/shopsystem-bc-launcher" in tokens
    assert "repository_dispatch" in tokens
    assert "framework-utility-release" in tokens
    assert "BC_LAUNCHER_DISPATCH_TOKEN" in tokens
    assert not no_emit_guarantee_holds(wf_dir), (
        "guarantee must NOT hold while the emit is present in the executable body"
    )


def test_scenario_passes_when_emit_removed(tmp_path: Path) -> None:
    # No workflows file at all -> absence satisfies the guarantee.
    wf_dir = tmp_path / ".github" / "workflows"
    wf_dir.mkdir(parents=True, exist_ok=True)
    assert forbidden_tokens_in_executable_body(wf_dir) == []
    assert no_emit_guarantee_holds(wf_dir)


def test_comment_only_emit_does_not_fail_guarantee(tmp_path: Path) -> None:
    wf_dir = _write_workflow(tmp_path, _COMMENT_ONLY_RELEASE_YML)
    # The tokens appear in the raw text...
    raw = (wf_dir / "release.yml").read_text()
    assert "BC_LAUNCHER_DISPATCH_TOKEN" in raw
    assert "repository_dispatch" in raw
    # ...but ONLY inside comments, so the comment-stripped executable body
    # is clean and the guarantee holds.
    assert forbidden_tokens_in_executable_body(wf_dir) == []
    assert no_emit_guarantee_holds(wf_dir)


def test_absent_workflows_tree_satisfies_guarantee(tmp_path: Path) -> None:
    # No .github/ directory at all.
    wf_dir = tmp_path / ".github" / "workflows"
    assert not wf_dir.exists()
    assert forbidden_tokens_in_executable_body(wf_dir) == []
    assert no_emit_guarantee_holds(wf_dir)
