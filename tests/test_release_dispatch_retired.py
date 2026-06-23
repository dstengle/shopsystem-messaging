"""Guard: the obsolete cross-repo repository_dispatch fan-in is retired.

Per ADR-022 (accepted 2026-06-09), the centralized bc-launcher scheduled
poll supersedes the per-repo `repository_dispatch` fan-in that
.github/workflows/release.yml used to emit (the `dispatch-bc-launcher-build`
job). lead-k6xq retired that job; this guard pins its absence so it cannot
silently regress.

The BC-register scenario @scenario_hash:b891abf0d7ce801f
(release_workflow_repository_dispatch_to_bc_launcher.feature, alias
a83760dcc40c57e6 in legacy comments) is retired with no successor: the
rebuild trigger moved to the centralized bc-launcher poll.
"""
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_RELEASE_WF = _REPO_ROOT / ".github" / "workflows" / "release.yml"


def test_release_workflow_exists_and_parses() -> None:
    assert _RELEASE_WF.is_file(), "release.yml is missing"
    spec = yaml.safe_load(_RELEASE_WF.read_text())
    assert isinstance(spec, dict), "release.yml did not parse to a mapping"


def test_release_workflow_has_no_cross_repo_dispatch() -> None:
    text = _RELEASE_WF.read_text()
    for forbidden in (
        "repository_dispatch",
        "shopsystem-bc-launcher/dispatches",
        "BC_LAUNCHER_DISPATCH_TOKEN",
        "dispatch-bc-launcher-build",
    ):
        assert forbidden not in text, (
            f"release.yml still references {forbidden!r}; the cross-repo "
            f"repository_dispatch fan-in must be retired (ADR-022)"
        )


def test_dispatch_bc_launcher_build_job_removed() -> None:
    spec = yaml.safe_load(_RELEASE_WF.read_text())
    jobs = spec.get("jobs") or {}
    assert "dispatch-bc-launcher-build" not in jobs, (
        "the dispatch-bc-launcher-build job must be removed entirely"
    )
