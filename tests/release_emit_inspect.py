"""Comment-stripped inspection of a GitHub Actions workflows tree for the
retired cross-repo bc-launcher repository_dispatch emit (ADR-022).

This is the shared mechanism behind the @scenario_hash:974ee23c53cbb09a
register scenario (work_id lead-n8pf) and its teeth-proof unit tests. The
no-emit guarantee is: with YAML comment lines EXCLUDED, the executable body
of every workflow declares no `dispatch-bc-launcher-build` job, no
`repository_dispatch` targeting `dstengle/shopsystem-bc-launcher`, no
`repository_dispatch` event_type `framework-utility-release`, and no secret
named `BC_LAUNCHER_DISPATCH_TOKEN`.

A token present ONLY inside a descriptive YAML comment (absent from the
comment-stripped executable body) does NOT violate the guarantee — comment
stripping is the load-bearing clause of scenario 974ee23c53cbb09a.

Absence of `.github/workflows/` (or an empty tree) trivially satisfies the
guarantee: absence of the emit IS the guarantee, not presence of a file.
"""
from pathlib import Path

# The forbidden executable-body tokens. Each names a distinct facet of the
# retired emit; the scenario asserts the absence of all of them.
FORBIDDEN_EXECUTABLE_TOKENS = (
    "dispatch-bc-launcher-build",
    "dstengle/shopsystem-bc-launcher",
    "repository_dispatch",
    "framework-utility-release",
    "BC_LAUNCHER_DISPATCH_TOKEN",
)


def strip_yaml_comments(text: str) -> str:
    """Return `text` with YAML comment lines/segments removed.

    A `#` inside a single- or double-quoted scalar is NOT a comment, so the
    stripper tracks quote state and only treats an unquoted `#` as the start
    of a comment. Whole-line and trailing comments are both removed. The
    executable body is what remains.
    """
    out_lines: list[str] = []
    for line in text.splitlines():
        result_chars: list[str] = []
        in_single = False
        in_double = False
        i = 0
        n = len(line)
        while i < n:
            ch = line[i]
            if in_single:
                result_chars.append(ch)
                if ch == "'":
                    in_single = False
            elif in_double:
                result_chars.append(ch)
                if ch == '"':
                    in_double = False
            else:
                if ch == "'":
                    in_single = True
                    result_chars.append(ch)
                elif ch == '"':
                    in_double = True
                    result_chars.append(ch)
                elif ch == "#":
                    # Unquoted '#' begins a comment: drop the rest of the line.
                    break
                else:
                    result_chars.append(ch)
            i += 1
        out_lines.append("".join(result_chars))
    return "\n".join(out_lines)


def _workflow_files(workflows_dir: Path) -> list[Path]:
    if not workflows_dir.is_dir():
        return []
    return sorted(workflows_dir.glob("*.yml")) + sorted(workflows_dir.glob("*.yaml"))


def forbidden_tokens_in_executable_body(workflows_dir: Path) -> list[tuple[str, str]]:
    """Return a list of (workflow filename, forbidden token) pairs present in
    the comment-stripped executable body across the workflows tree.

    An empty list means the no-emit guarantee holds for this tree (including
    the absent-tree / empty-tree case).
    """
    violations: list[tuple[str, str]] = []
    for wf in _workflow_files(workflows_dir):
        executable_body = strip_yaml_comments(wf.read_text())
        for token in FORBIDDEN_EXECUTABLE_TOKENS:
            if token in executable_body:
                violations.append((wf.name, token))
    return violations


def no_emit_guarantee_holds(workflows_dir: Path) -> bool:
    """True iff the comment-stripped executable body of every workflow in
    `workflows_dir` is free of the retired bc-launcher dispatch emit."""
    return not forbidden_tokens_in_executable_body(workflows_dir)
