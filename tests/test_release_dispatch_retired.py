"""Guard: the obsolete cross-repo repository_dispatch fan-in is retired.

Per ADR-022 (accepted 2026-06-09), the centralized bc-launcher scheduled
poll supersedes the per-repo `repository_dispatch` fan-in that
.github/workflows/release.yml used to emit (the `dispatch-bc-launcher-build`
job). lead-k6xq retired that job — the whole release.yml existed solely to
emit it and carried no version-hygiene guard to keep, so the workflow file
was removed. This guard pins the absence of the dispatch wiring across the
entire workflows tree so it cannot silently regress.

The BC-register scenario @scenario_hash:b891abf0d7ce801f
(release_workflow_repository_dispatch_to_bc_launcher.feature, alias
a83760dcc40c57e6 in legacy comments) is retired with no successor: the
rebuild trigger moved to the centralized bc-launcher poll.
"""
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_WORKFLOWS_DIR = _REPO_ROOT / ".github" / "workflows"

_FORBIDDEN_TOKENS = (
    "repository_dispatch",
    "shopsystem-bc-launcher/dispatches",
    "BC_LAUNCHER_DISPATCH_TOKEN",
    "dispatch-bc-launcher-build",
)


def _workflow_files() -> list[Path]:
    if not _WORKFLOWS_DIR.is_dir():
        return []
    return sorted(_WORKFLOWS_DIR.glob("*.yml")) + sorted(
        _WORKFLOWS_DIR.glob("*.yaml")
    )


def test_no_workflow_carries_cross_repo_dispatch() -> None:
    for wf in _workflow_files():
        text = wf.read_text()
        for forbidden in _FORBIDDEN_TOKENS:
            assert forbidden not in text, (
                f"{wf.name} still references {forbidden!r}; the cross-repo "
                f"repository_dispatch fan-in must be retired (ADR-022)"
            )


def test_no_dispatch_bc_launcher_build_job() -> None:
    for wf in _workflow_files():
        spec = yaml.safe_load(wf.read_text())
        if not isinstance(spec, dict):
            continue
        jobs = spec.get("jobs") or {}
        assert "dispatch-bc-launcher-build" not in jobs, (
            f"{wf.name} still defines the dispatch-bc-launcher-build job; it "
            f"must be removed entirely (ADR-022)"
        )


def test_all_workflows_still_parse() -> None:
    # Whatever workflows remain must be valid YAML mappings (catches a
    # botched deletion leaving an invalid file behind).
    for wf in _workflow_files():
        spec = yaml.safe_load(wf.read_text())
        assert isinstance(spec, dict), f"{wf.name} did not parse to a mapping"
