"""Shared fixture and import setup for pytest-bdd in shop-msg-bc.

Step definitions themselves are written by the Implementer as part of
the work for each `assign_scenarios` message — new phrasings produce
new step definitions here. Schemas come from the installed `catalog`
package; the CLI is invoked via the installed `shop-msg` console script.

Storage backend: All messages are stored in Postgres (psycopg v3).
The SHOPMSG_DSN environment variable controls the connection; the
default DSN is set for the development/CI environment. Each test
gets its own bc_root (a unique tmp_path), which acts as the Postgres
namespace key so tests are isolated without any extra cleanup.
"""
import json
import os
import subprocess
from pathlib import Path
from typing import Any

# Point the test suite at the local dev Postgres cluster (/tmp/pgrun socket,
# port 5433) rather than the Docker Compose service name.  The compose
# service is production infra; during development and CI the cluster is
# managed by the devcontainer init scripts at host=/tmp/pgrun port=5433.
# This override must be set before any storage module is imported so the
# module-level _DEFAULT_DSN is bypassed uniformly for all tests.
os.environ.setdefault(
    "SHOPMSG_DSN", "host=/tmp/pgrun port=5433 dbname=shopsystem user=vscode"
)

import psycopg
import pytest
import yaml
from pytest_bdd import given, parsers, then, when

from pydantic import ValidationError

from catalog.schemas import (
    AssignScenarios,
    Clarify,
    MechanismObservation,
    RequestBugfix,
    RequestMaintenance,
    ScenarioPayload,
    WorkDone,
)
from shop_msg.storage import (
    _bc_id,
    _connect,
    delete_bc_messages,
    inbox_row_exists,
    insert_raw_payload,
    outbox_row_exists,
    read_inbox_message,
    read_outbox_messages,
)


@pytest.fixture
def context() -> dict:
    return {}


@given("an empty BC at a temporary path", target_fixture="bc_root")
def empty_bc(tmp_path: Path) -> Path:
    # The directories are not used for storage (Postgres holds messages),
    # but some step definitions reference bc_root as a path concept and
    # the CLI resolves it to an absolute path. We create the dirs so
    # Path.resolve() works and legacy step logic that checks bc_root.exists()
    # doesn't fail unexpectedly.
    (tmp_path / "inbox").mkdir()
    (tmp_path / "outbox").mkdir()
    return tmp_path


# ---------------------------------------------------------------------------
# Helpers for reading from the Postgres messages table in tests
# ---------------------------------------------------------------------------

def _fetch_outbox_rows(bc_root: Path) -> list[dict]:
    """Return all outbox rows for this bc_root, ordered by created_at."""
    bc = _bc_id(str(bc_root.resolve()))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT work_id, message_type, payload
                FROM messages
                WHERE bc = %s AND direction = 'outbox'
                ORDER BY created_at
                """,
                (bc,),
            )
            rows = cur.fetchall()
    result = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        result.append({
            "work_id": row["work_id"],
            "message_type": row["message_type"],
            "payload": payload,
        })
    return result


def _fetch_inbox_rows(bc_root: Path) -> list[dict]:
    """Return all inbox rows for this bc_root, ordered by created_at."""
    bc = _bc_id(str(bc_root.resolve()))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT work_id, message_type, payload
                FROM messages
                WHERE bc = %s AND direction = 'inbox'
                ORDER BY created_at
                """,
                (bc,),
            )
            rows = cur.fetchall()
    result = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        result.append({
            "work_id": row["work_id"],
            "message_type": row["message_type"],
            "payload": payload,
        })
    return result


def _fetch_outbox_payload(bc_root: Path, work_id: str, message_type: str) -> dict | None:
    """Return the payload for a specific outbox row, or None."""
    bc = _bc_id(str(bc_root.resolve()))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload FROM messages
                WHERE bc = %s AND work_id = %s
                  AND direction = 'outbox' AND message_type = %s
                LIMIT 1
                """,
                (bc, work_id, message_type),
            )
            row = cur.fetchone()
    if row is None:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


# ---------------------------------------------------------------------------
# Filename-to-(work_id, message_type) parsing
# ---------------------------------------------------------------------------

_OUTBOX_RESPONSE_TYPES = ("clarify", "work_done", "mechanism_observation")
_INBOX_SUFFIX = ".yaml"


def _parse_outbox_filename(filename: str) -> tuple[str, str]:
    """Parse 'lead-001-clarify.yaml' -> ('lead-001', 'clarify').

    The filename convention is <work_id>-<message_type>.yaml where
    message_type is one of the known response types.
    """
    stem = filename
    if stem.endswith(".yaml"):
        stem = stem[:-5]
    for rt in _OUTBOX_RESPONSE_TYPES:
        suffix = f"-{rt}"
        if stem.endswith(suffix):
            return stem[: -len(suffix)], rt
    # Fallback: no recognized response type suffix
    return stem, "unknown"


def _parse_inbox_filename(filename: str) -> str:
    """Parse 'lead-001.yaml' -> 'lead-001'."""
    if filename.endswith(".yaml"):
        return filename[:-5]
    return filename


# ---------------------------------------------------------------------------
# Preexisting outbox file setup (collision tests)
# ---------------------------------------------------------------------------

@given(parsers.parse('the BC\'s outbox already contains a file named "{filename}"'))
def outbox_preexisting_file(bc_root: Path, filename: str, context: dict) -> None:
    # In the Postgres backend, "a file named X" maps to an outbox row.
    # We insert a sentinel payload so the collision check can verify
    # the row wasn't overwritten.
    work_id, message_type = _parse_outbox_filename(filename)
    sentinel_payload: dict[str, Any] = {
        "message_type": message_type,
        "work_id": work_id,
        "_sentinel": True,
        "preexisting": True,
    }
    # Insert via the raw helper (bypasses schema validation intentionally —
    # the collision test only cares the row exists, not that it's valid).
    insert_raw_payload(
        str(bc_root.resolve()),
        work_id,
        "outbox",
        message_type,
        sentinel_payload,
    )
    # Store the sentinel so the "unchanged" step can compare later.
    context["preexisting_files"] = context.get("preexisting_files", {})
    context["preexisting_files"][filename] = sentinel_payload.copy()


@when(
    parsers.re(
        r'I run shop-msg respond clarify with work-id "(?P<work_id>[^"]*)" '
        r'and question "(?P<question>[^"]*)"'
    )
)
def run_respond_clarify(bc_root: Path, work_id: str, question: str, context: dict) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "respond",
            "clarify",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--question",
            question,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then("the command exits non-zero")
def command_exits_nonzero(context: dict) -> None:
    rc = context["cli_returncode"]
    assert rc != 0, f"expected non-zero exit; got {rc}; stderr:\n{context.get('cli_stderr', '')}"


@then(parsers.parse('the BC\'s outbox contains a file named "{filename}"'))
def outbox_contains_file(bc_root: Path, filename: str, context: dict) -> None:
    # After the Postgres swap "a file named X" means an outbox DB row
    # with the work_id and message_type decoded from the filename.
    rc = context.get("cli_returncode")
    assert rc == 0, (
        f"shop-msg exited {rc}; stderr:\n{context.get('cli_stderr', '')}"
    )
    work_id, message_type = _parse_outbox_filename(filename)
    payload = _fetch_outbox_payload(bc_root, work_id, message_type)
    assert payload is not None, (
        f"expected outbox row for work_id={work_id!r} message_type={message_type!r}; "
        f"outbox rows: {[r['work_id'] for r in _fetch_outbox_rows(bc_root)]}"
    )
    context["outbox_payload"] = payload
    # Store a synthetic "file" path for backward-compat with Then-steps
    # that call `context['outbox_file']`. We write a temp YAML to let
    # the downstream Then-step parse it if needed — but prefer checking
    # `outbox_payload` directly.
    # Actually, maintain outbox_file as a temp path for those Then-steps
    # that open it (file_parses_as_clarify etc). We write a temp file.
    import tempfile, os
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False,
        prefix=f"shopmsg_outbox_{work_id}_"
    )
    yaml.safe_dump(payload, tmp, sort_keys=False)
    tmp.close()
    context["outbox_file"] = Path(tmp.name)
    context["_outbox_tmpfiles"] = context.get("_outbox_tmpfiles", [])
    context["_outbox_tmpfiles"].append(tmp.name)


@then(parsers.parse('the BC\'s outbox file "{filename}" is unchanged'))
def outbox_file_unchanged(bc_root: Path, filename: str, context: dict) -> None:
    # Verify the outbox DB row for this filename still has the sentinel
    # payload that was inserted in the Given step.
    work_id, message_type = _parse_outbox_filename(filename)
    original = context["preexisting_files"][filename]
    actual_payload = _fetch_outbox_payload(bc_root, work_id, message_type)
    assert actual_payload is not None, (
        f"expected outbox row for {filename} to still exist after failed write"
    )
    # Compare only the sentinel marker — the key indicator that the row
    # was not overwritten with a new payload.
    assert actual_payload.get("_sentinel") is True, (
        f"expected outbox row to retain sentinel payload; "
        f"actual payload: {actual_payload!r}"
    )


@then("the BC's outbox is empty")
def outbox_is_empty(bc_root: Path) -> None:
    rows = _fetch_outbox_rows(bc_root)
    assert rows == [], (
        f"expected no outbox rows for bc={bc_root}; "
        f"found: {[(r['work_id'], r['message_type']) for r in rows]}"
    )


@then(
    parsers.parse(
        'the file parses as a valid Clarify with work_id "{work_id}" and question "{question}"'
    )
)
def file_parses_as_clarify(context: dict, work_id: str, question: str) -> None:
    payload = context.get("outbox_payload") or yaml.safe_load(
        context["outbox_file"].read_text()
    )
    msg = Clarify(**payload)
    assert msg.work_id == work_id
    assert msg.question == question


@when(
    parsers.re(
        r'I run shop-msg respond work_done with work-id "(?P<work_id>[^"]*)" '
        r'and status "(?P<status>[^"]*)" and scenario-hash "(?P<scenario_hash>[^"]*)"'
    )
)
def run_respond_work_done_with_hash(
    bc_root: Path, work_id: str, status: str, scenario_hash: str, context: dict
) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "respond",
            "work_done",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--status",
            status,
            "--scenario-hash",
            scenario_hash,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@when(
    parsers.re(
        r'I run shop-msg respond work_done with work-id "(?P<work_id>[^"]*)" '
        r'and status "(?P<status>[^"]*)"$'
    )
)
def run_respond_work_done_no_hash(
    bc_root: Path, work_id: str, status: str, context: dict
) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "respond",
            "work_done",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--status",
            status,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then(
    parsers.parse(
        'the file parses as a valid WorkDone with work_id "{work_id}" and status "{status}"'
    )
)
def file_parses_as_work_done(context: dict, work_id: str, status: str) -> None:
    payload = context.get("outbox_payload") or yaml.safe_load(
        context["outbox_file"].read_text()
    )
    msg = WorkDone(**payload)
    assert msg.work_id == work_id
    assert msg.status == status


# ---------------------------------------------------------------------------
# Preexisting inbox file setup (collision tests)
# ---------------------------------------------------------------------------

@given(parsers.parse('the BC\'s inbox already contains a file named "{filename}"'))
def inbox_preexisting_file(bc_root: Path, filename: str, context: dict) -> None:
    # Decode work_id from filename (inbox files are <work_id>.yaml).
    work_id = _parse_inbox_filename(filename)
    sentinel_payload: dict[str, Any] = {
        "message_type": "request_maintenance",
        "work_id": work_id,
        "_sentinel": True,
        "preexisting": True,
        "description": "sentinel preexisting",
    }
    insert_raw_payload(
        str(bc_root.resolve()),
        work_id,
        "inbox",
        "request_maintenance",
        sentinel_payload,
    )
    context["preexisting_inbox_files"] = context.get("preexisting_inbox_files", {})
    context["preexisting_inbox_files"][filename] = sentinel_payload.copy()


@when(
    parsers.re(
        r'I run shop-msg send request_maintenance with work-id "(?P<work_id>[^"]*)" '
        r'and description "(?P<description>[^"]*)"$'
    )
)
def run_send_request_maintenance(
    bc_root: Path, work_id: str, description: str, context: dict
) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "send",
            "request_maintenance",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--description",
            description,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@when(
    parsers.re(
        r'I run shop-msg send request_maintenance with work-id "(?P<work_id>[^"]*)" '
        r'and description "(?P<description>[^"]*)" '
        r'and acceptance-criterion "(?P<criterion>[^"]*)" '
        r'and file-hint "(?P<file_hint>[^"]*)"$'
    )
)
def run_send_request_maintenance_with_criterion_and_hint(
    bc_root: Path,
    work_id: str,
    description: str,
    criterion: str,
    file_hint: str,
    context: dict,
) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "send",
            "request_maintenance",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--description",
            description,
            "--acceptance-criterion",
            criterion,
            "--file-hint",
            file_hint,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@when(
    parsers.re(
        r'I run shop-msg send request_maintenance with work-id "(?P<work_id>[^"]*)" '
        r'and description "(?P<description>[^"]*)" '
        r'and acceptance-criterion "(?P<criterion1>[^"]*)" '
        r'and acceptance-criterion "(?P<criterion2>[^"]*)"$'
    )
)
def run_send_request_maintenance_with_two_criteria(
    bc_root: Path,
    work_id: str,
    description: str,
    criterion1: str,
    criterion2: str,
    context: dict,
) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "send",
            "request_maintenance",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--description",
            description,
            "--acceptance-criterion",
            criterion1,
            "--acceptance-criterion",
            criterion2,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then(parsers.parse('the BC\'s inbox contains a file named "{filename}"'))
def inbox_contains_file(bc_root: Path, filename: str, context: dict) -> None:
    # After the Postgres swap "a file named X" means an inbox DB row.
    rc = context.get("cli_returncode")
    assert rc == 0, (
        f"shop-msg exited {rc}; stderr:\n{context.get('cli_stderr', '')}"
    )
    work_id = _parse_inbox_filename(filename)
    inbox_rows = _fetch_inbox_rows(bc_root)
    matching = [r for r in inbox_rows if r["work_id"] == work_id]
    assert matching, (
        f"expected inbox row for work_id={work_id!r}; "
        f"found work_ids: {[r['work_id'] for r in inbox_rows]}"
    )
    context["inbox_payload"] = matching[-1]["payload"]
    # Write temp YAML for downstream Then-steps that open inbox_file.
    import tempfile
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False,
        prefix=f"shopmsg_inbox_{work_id}_"
    )
    yaml.safe_dump(context["inbox_payload"], tmp, sort_keys=False)
    tmp.close()
    context["inbox_file"] = Path(tmp.name)


@then(
    parsers.parse(
        'the file parses as a valid RequestMaintenance with work_id "{work_id}" '
        'and description "{description}"'
    )
)
def file_parses_as_request_maintenance(
    context: dict, work_id: str, description: str
) -> None:
    payload = context.get("inbox_payload") or yaml.safe_load(
        context["inbox_file"].read_text()
    )
    msg = RequestMaintenance(**payload)
    assert msg.work_id == work_id
    assert msg.description == description


def _parse_quoted_list(raw: str) -> list[str]:
    """Parse a Then-step list literal like '["a", "b"]' into ['a', 'b'].

    Tolerates the simple shape used by these scenarios: bracket-delimited,
    comma-separated, double-quoted strings.
    """
    import re as _re
    return _re.findall(r'"([^"]*)"', raw)


@then(
    parsers.re(
        r'the file parses as a valid RequestMaintenance with work_id "(?P<work_id>[^"]*)", '
        r'description "(?P<description>[^"]*)", '
        r'acceptance_criteria (?P<criteria>\[[^\]]*\]), '
        r'and file_hints (?P<hints>\[[^\]]*\])$'
    )
)
def file_parses_as_request_maintenance_full(
    context: dict,
    work_id: str,
    description: str,
    criteria: str,
    hints: str,
) -> None:
    payload = context.get("inbox_payload") or yaml.safe_load(
        context["inbox_file"].read_text()
    )
    msg = RequestMaintenance(**payload)
    assert msg.work_id == work_id
    assert msg.description == description
    assert msg.acceptance_criteria == _parse_quoted_list(criteria)
    assert msg.file_hints == _parse_quoted_list(hints)


@then(
    parsers.re(
        r'the file parses as a valid RequestMaintenance with work_id "(?P<work_id>[^"]*)" '
        r'and acceptance_criteria (?P<criteria>\[[^\]]*\])$'
    )
)
def file_parses_as_request_maintenance_with_criteria(
    context: dict,
    work_id: str,
    criteria: str,
) -> None:
    payload = context.get("inbox_payload") or yaml.safe_load(
        context["inbox_file"].read_text()
    )
    msg = RequestMaintenance(**payload)
    assert msg.work_id == work_id
    assert msg.acceptance_criteria == _parse_quoted_list(criteria)


@then(parsers.parse('the BC\'s inbox file "{filename}" is unchanged'))
def inbox_file_unchanged(bc_root: Path, filename: str, context: dict) -> None:
    work_id = _parse_inbox_filename(filename)
    original = context["preexisting_inbox_files"][filename]
    bc = _bc_id(str(bc_root.resolve()))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload FROM messages
                WHERE bc = %s AND work_id = %s AND direction = 'inbox'
                LIMIT 1
                """,
                (bc, work_id),
            )
            row = cur.fetchone()
    assert row is not None, f"expected inbox row for {filename} to still exist"
    actual_payload = row["payload"]
    if isinstance(actual_payload, str):
        actual_payload = json.loads(actual_payload)
    assert actual_payload.get("_sentinel") is True, (
        f"expected inbox row to retain sentinel payload; "
        f"actual: {actual_payload!r}"
    )


def _scenario_hash_via_cli(body: str) -> str:
    """Invoke the `scenarios hash` CLI to compute the canonical hash.

    Tests deliberately go through the same CLI boundary the production
    code uses, so a regression in either side surfaces here.
    """
    result = subprocess.run(
        ["scenarios", "hash"],
        input=body,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _write_scenario_body_file(tmp_path: Path, context: dict, raw_text: str) -> Path:
    """Materialize a scenario body to a file under tmp_path.

    The Gherkin step text encodes newlines as the literal two-character
    escape ``\\n``; this helper converts them back to real newlines so
    the file mirrors what a user would author by hand.
    """
    body = raw_text.replace("\\n", "\n")
    files = context.setdefault("scenario_body_files", [])
    files.append({"body": body})
    idx = len(files) - 1
    path = tmp_path / f"scenario_body_{idx}.txt"
    path.write_text(body)
    files[idx]["path"] = path
    return path


@given(
    parsers.parse('a scenario body file containing the text "{raw_text}"')
)
def given_scenario_body_file(tmp_path: Path, context: dict, raw_text: str) -> None:
    _write_scenario_body_file(tmp_path, context, raw_text)


@given(
    parsers.parse('another scenario body file containing the text "{raw_text}"')
)
def given_another_scenario_body_file(
    tmp_path: Path, context: dict, raw_text: str
) -> None:
    _write_scenario_body_file(tmp_path, context, raw_text)


@when(
    parsers.re(
        r'I run shop-msg send assign_scenarios with work-id "(?P<work_id>[^"]*)" '
        r'and feature-title "(?P<feature_title>[^"]*)" '
        r'and bc-tag "(?P<bc_tag>[^"]*)" '
        r'and that scenario file$'
    )
)
def run_send_assign_scenarios_one_file(
    bc_root: Path,
    work_id: str,
    feature_title: str,
    bc_tag: str,
    context: dict,
) -> None:
    files = context["scenario_body_files"]
    assert len(files) == 1, (
        f"'that scenario file' expects exactly one scenario body file; got {len(files)}"
    )
    cmd = [
        "shop-msg",
        "send",
        "assign_scenarios",
        "--bc-root",
        str(bc_root),
        "--work-id",
        work_id,
        "--feature-title",
        feature_title,
        "--bc-tag",
        bc_tag,
        "--scenario-file",
        str(files[0]["path"]),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@when(
    parsers.re(
        r'I run shop-msg send assign_scenarios with work-id "(?P<work_id>[^"]*)" '
        r'and feature-title "(?P<feature_title>[^"]*)" '
        r'and bc-tag "(?P<bc_tag>[^"]*)" '
        r'and both scenario files$'
    )
)
def run_send_assign_scenarios_both_files(
    bc_root: Path,
    work_id: str,
    feature_title: str,
    bc_tag: str,
    context: dict,
) -> None:
    files = context["scenario_body_files"]
    assert len(files) == 2, (
        f"'both scenario files' expects exactly two scenario body files; got {len(files)}"
    )
    cmd = [
        "shop-msg",
        "send",
        "assign_scenarios",
        "--bc-root",
        str(bc_root),
        "--work-id",
        work_id,
        "--feature-title",
        feature_title,
        "--bc-tag",
        bc_tag,
    ]
    for entry in files:
        cmd.extend(["--scenario-file", str(entry["path"])])
    result = subprocess.run(cmd, capture_output=True, text=True)
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then(
    parsers.parse(
        'the file parses as a valid AssignScenarios with work_id "{work_id}" '
        'and one scenario whose hash equals the scenarios-hash of the body'
    )
)
def file_parses_as_assign_scenarios_one_with_hash_match(
    context: dict, work_id: str
) -> None:
    payload = context.get("inbox_payload") or yaml.safe_load(
        context["inbox_file"].read_text()
    )
    msg = AssignScenarios(**payload)
    assert msg.work_id == work_id
    assert len(msg.scenarios) == 1, (
        f"expected exactly one scenario in payload; got {len(msg.scenarios)}"
    )
    body = context["scenario_body_files"][0]["body"]
    actual_hash = msg.scenarios[0].hash
    gherkin = msg.scenarios[0].gherkin
    first_body_line = next(
        (l for l in body.splitlines() if l.strip()), ""
    )
    assert first_body_line in gherkin, (
        f"expected the body's first non-blank line {first_body_line!r} "
        f"to appear in the gherkin; got gherkin:\n{gherkin}"
    )
    expected_hash = _scenario_hash_via_cli(gherkin)
    assert actual_hash == expected_hash, (
        f"scenario hash mismatch: CLI emitted {actual_hash!r}, "
        f"`scenarios hash` of gherkin produces {expected_hash!r}"
    )


@then(
    parsers.parse(
        'the file parses as a valid AssignScenarios with work_id "{work_id}" '
        'and two scenarios whose hashes are distinct'
    )
)
def file_parses_as_assign_scenarios_two_distinct(
    context: dict, work_id: str
) -> None:
    payload = context.get("inbox_payload") or yaml.safe_load(
        context["inbox_file"].read_text()
    )
    msg = AssignScenarios(**payload)
    assert msg.work_id == work_id
    assert len(msg.scenarios) == 2, (
        f"expected exactly two scenarios in payload; got {len(msg.scenarios)}"
    )
    h0, h1 = msg.scenarios[0].hash, msg.scenarios[1].hash
    assert h0 != h1, f"expected distinct hashes; both were {h0!r}"


@when(
    parsers.re(
        r'I run shop-msg send request_bugfix with work-id "(?P<work_id>[^"]*)" '
        r'and description "(?P<description>[^"]*)"$'
    )
)
def run_send_request_bugfix_description_only(
    bc_root: Path, work_id: str, description: str, context: dict
) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "send",
            "request_bugfix",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--description",
            description,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@when(
    parsers.re(
        r'I run shop-msg send request_bugfix with work-id "(?P<work_id>[^"]*)", '
        r'description "(?P<description>[^"]*)", '
        r'feature-title "(?P<feature_title>[^"]*)", '
        r'bc-tag "(?P<bc_tag>[^"]*)", '
        r'and that scenario file$'
    )
)
def run_send_request_bugfix_with_one_scenario(
    bc_root: Path,
    work_id: str,
    description: str,
    feature_title: str,
    bc_tag: str,
    context: dict,
) -> None:
    files = context["scenario_body_files"]
    assert len(files) == 1, (
        f"'that scenario file' expects exactly one scenario body file; got {len(files)}"
    )
    cmd = [
        "shop-msg",
        "send",
        "request_bugfix",
        "--bc-root",
        str(bc_root),
        "--work-id",
        work_id,
        "--description",
        description,
        "--feature-title",
        feature_title,
        "--bc-tag",
        bc_tag,
        "--scenario-file",
        str(files[0]["path"]),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then(
    parsers.parse(
        'the file parses as a valid RequestBugfix with work_id "{work_id}", '
        'description "{description}", and no scenarios'
    )
)
def file_parses_as_request_bugfix_no_scenarios(
    context: dict, work_id: str, description: str
) -> None:
    payload = context.get("inbox_payload") or yaml.safe_load(
        context["inbox_file"].read_text()
    )
    msg = RequestBugfix(**payload)
    assert msg.work_id == work_id
    assert msg.description == description
    assert msg.scenarios == [], (
        f"expected no scenarios; got {len(msg.scenarios)}"
    )


@given(
    parsers.re(
        r'shop-msg respond work_done was previously used to write '
        r'"(?P<filename>[^"]*)" with status "(?P<status>[^"]*)" '
        r'and scenario-hash "(?P<scenario_hash>[^"]*)"'
    )
)
def given_prior_work_done(
    bc_root: Path,
    filename: str,
    status: str,
    scenario_hash: str,
) -> None:
    # Filename of the form "<work_id>-work_done.yaml"; recover work_id.
    suffix = "-work_done.yaml"
    assert filename.endswith(suffix), (
        f"expected work_done filename to end with {suffix!r}; got {filename!r}"
    )
    work_id = filename[: -len(suffix)]
    subprocess.run(
        [
            "shop-msg",
            "respond",
            "work_done",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--status",
            status,
            "--scenario-hash",
            scenario_hash,
        ],
        capture_output=True,
        text=True,
        check=True,
    )


@given(
    parsers.re(
        r'shop-msg respond clarify was previously used to write '
        r'"(?P<filename>[^"]*)" with question "(?P<question>[^"]*)"'
    )
)
def given_prior_clarify(bc_root: Path, filename: str, question: str) -> None:
    suffix = "-clarify.yaml"
    assert filename.endswith(suffix), (
        f"expected clarify filename to end with {suffix!r}; got {filename!r}"
    )
    work_id = filename[: -len(suffix)]
    subprocess.run(
        [
            "shop-msg",
            "respond",
            "clarify",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
            "--question",
            question,
        ],
        capture_output=True,
        text=True,
        check=True,
    )


@when(
    parsers.re(
        r'I run shop-msg read outbox with work-id "(?P<work_id>[^"]*)"$'
    )
)
def run_read_outbox(bc_root: Path, work_id: str, context: dict) -> None:
    result = subprocess.run(
        [
            "shop-msg",
            "read",
            "outbox",
            "--bc-root",
            str(bc_root),
            "--work-id",
            work_id,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then("the command exits zero")
def command_exits_zero(context: dict) -> None:
    rc = context["cli_returncode"]
    assert rc == 0, (
        f"expected zero exit; got {rc}; stderr:\n{context.get('cli_stderr', '')}"
    )


@then(
    parsers.re(
        r'stdout includes message_type "(?P<message_type>[^"]*)" '
        r'and work_id "(?P<work_id>[^"]*)" '
        r'and status "(?P<status>[^"]*)"$'
    )
)
def stdout_includes_message_type_work_id_status(
    context: dict, message_type: str, work_id: str, status: str
) -> None:
    stdout = context.get("cli_stdout", "")
    for token in (message_type, work_id, status):
        assert token in stdout, (
            f"expected stdout to contain {token!r}; full stdout:\n{stdout}"
        )


@then(
    parsers.re(
        r'stdout includes message_type "(?P<message_type>[^"]*)" '
        r'and work_id "(?P<work_id>[^"]*)"$'
    )
)
def stdout_includes_message_type_and_work_id(
    context: dict, message_type: str, work_id: str
) -> None:
    stdout = context.get("cli_stdout", "")
    for token in (message_type, work_id):
        assert token in stdout, (
            f"expected stdout to contain {token!r}; full stdout:\n{stdout}"
        )


@then("stderr explains no outbox response was found")
def stderr_explains_no_outbox_response(context: dict) -> None:
    stderr = context.get("cli_stderr", "")
    assert "no outbox response" in stderr, (
        f"expected stderr to explain no outbox response was found; got:\n{stderr}"
    )


@given(
    parsers.parse(
        'the BC\'s outbox already contains a file named "{filename}" with content '
        'that is valid YAML but does not match the BCResponse schema'
    )
)
def outbox_preexisting_invalid_response(
    bc_root: Path, filename: str, context: dict
) -> None:
    # Insert an invalid payload (valid JSON/JSONB but fails BCResponse schema).
    work_id, message_type = _parse_outbox_filename(filename)
    invalid_payload = {
        "message_type": "not_a_real_type",
        "work_id": "lead-099",
        "question": "this payload is structurally valid YAML",
    }
    insert_raw_payload(
        str(bc_root.resolve()),
        work_id,
        "outbox",
        message_type,
        invalid_payload,
    )


@then("stderr explains schema validation failed")
def stderr_explains_schema_validation_failed(context: dict) -> None:
    stderr = context.get("cli_stderr", "")
    assert "validation failed" in stderr, (
        f"expected stderr to explain schema validation failed; got:\n{stderr}"
    )


@then(
    parsers.parse(
        'the BC\'s inbox file contains a gherkin string with a line '
        'containing "{needle}"'
    )
)
def inbox_file_gherkin_contains(bc_root: Path, needle: str, context: dict) -> None:
    rc = context.get("cli_returncode")
    assert rc == 0, (
        f"shop-msg exited {rc}; stderr:\n{context.get('cli_stderr', '')}"
    )
    # Read all inbox rows from DB for this bc_root.
    inbox_rows = _fetch_inbox_rows(bc_root)
    assert inbox_rows, (
        f"expected at least one inbox row after send; got none"
    )
    # Look for a row whose scenarios payload contains a gherkin line with needle.
    found = False
    for row in inbox_rows:
        payload = row["payload"]
        scenarios_field = payload.get("scenarios") or []
        for sp in scenarios_field:
            gherkin = sp.get("gherkin", "")
            for line in gherkin.splitlines():
                if needle in line:
                    found = True
                    break
            if found:
                break
        if found:
            break
    assert found, (
        f"expected some scenario's gherkin to contain a line with {needle!r}; "
        f"inbox rows: {inbox_rows!r}"
    )


@then(
    parsers.re(
        r'the BC\'s inbox file "(?P<filename>[^"]*)" parses as a valid '
        r'RequestBugfix with description "(?P<description>[^"]*)" '
        r'and one scenario whose hash equals the scenarios-hash of the body$'
    )
)
def inbox_file_parses_as_request_bugfix_one_scenario(
    bc_root: Path, filename: str, description: str, context: dict
) -> None:
    rc = context.get("cli_returncode")
    assert rc == 0, (
        f"shop-msg exited {rc}; stderr:\n{context.get('cli_stderr', '')}"
    )
    work_id = _parse_inbox_filename(filename)
    raw = read_inbox_message(str(bc_root.resolve()), work_id)
    assert raw is not None, (
        f"expected inbox row for work_id={work_id!r}; "
        f"found: {[r['work_id'] for r in _fetch_inbox_rows(bc_root)]}"
    )
    msg = RequestBugfix(**raw)
    assert msg.description == description
    assert len(msg.scenarios) == 1, (
        f"expected exactly one scenario in payload; got {len(msg.scenarios)}"
    )
    body = context["scenario_body_files"][0]["body"]
    actual_hash = msg.scenarios[0].hash
    gherkin = msg.scenarios[0].gherkin
    first_body_line = next(
        (l for l in body.splitlines() if l.strip()), ""
    )
    assert first_body_line in gherkin, (
        f"expected the body's first non-blank line {first_body_line!r} "
        f"to appear in the gherkin; got gherkin:\n{gherkin}"
    )
    expected_hash = _scenario_hash_via_cli(gherkin)
    assert actual_hash == expected_hash, (
        f"scenario hash mismatch: CLI emitted {actual_hash!r}, "
        f"`scenarios hash` of gherkin produces {expected_hash!r}"
    )


# -----------------------------------------------------------------------
# lead-018: hash↔body schema invariant
# -----------------------------------------------------------------------

@given(
    parsers.parse(
        'a gherkin body that contains a "{bc_token}" tag line'
    ),
    target_fixture="gherkin_body",
)
def given_gherkin_body_with_bc_tag(bc_token: str) -> str:
    return (
        f"{bc_token}\n"
        f"Scenario: hash-matches-body construction\n"
        f"    Given a well-formed scenario body\n"
        f"    When I hash the body canonically\n"
        f"    Then the resulting payload validates\n"
    )


@given(
    "a hash value equal to the canonical scenario-hash of that gherkin",
    target_fixture="hash_value",
)
def given_matching_hash(gherkin_body: str) -> str:
    return _scenario_hash_via_cli(gherkin_body)


@given(
    "a hash value that does not equal the canonical scenario-hash of that gherkin",
    target_fixture="hash_value",
)
def given_mismatched_hash(gherkin_body: str) -> str:
    wrong = "0000000000000000"
    canonical = _scenario_hash_via_cli(gherkin_body)
    assert wrong != canonical, (
        f"wrong hash {wrong!r} collided with canonical hash; pick another"
    )
    return wrong


@when(
    "I construct a ScenarioPayload with that hash and that gherkin",
)
def when_construct_scenario_payload(
    gherkin_body: str, hash_value: str, context: dict
) -> None:
    payload = ScenarioPayload(hash=hash_value, gherkin=gherkin_body)
    context["scenario_payload"] = payload


@when(
    "I construct a ScenarioPayload with that hash and that gherkin via Pydantic",
)
def when_construct_scenario_payload_expecting_error(
    gherkin_body: str, hash_value: str, context: dict
) -> None:
    try:
        ScenarioPayload(hash=hash_value, gherkin=gherkin_body)
    except ValidationError as exc:
        context["validation_error"] = exc
        return
    context["validation_error"] = None


@then(
    "construction succeeds and the parsed model has the gherkin and hash intact",
)
def then_construction_succeeds(
    gherkin_body: str, hash_value: str, context: dict
) -> None:
    payload: ScenarioPayload = context["scenario_payload"]
    assert payload.gherkin == gherkin_body
    assert payload.hash == hash_value


@then("Pydantic raises ValidationError")
def then_pydantic_raises_validation_error(context: dict) -> None:
    exc = context.get("validation_error")
    assert exc is not None, (
        "expected ScenarioPayload(...) to raise ValidationError; "
        "construction returned successfully"
    )
    assert isinstance(exc, ValidationError), (
        f"expected ValidationError; got {type(exc).__name__}: {exc!r}"
    )


@then(
    "the error message identifies that the hash does not match the gherkin body",
)
def then_error_identifies_hash_mismatch(context: dict) -> None:
    exc: ValidationError = context["validation_error"]
    msg = str(exc)
    assert "hash" in msg, f"expected error to mention hash; got:\n{msg}"
    assert "canonical" in msg or "does not match" in msg, (
        f"expected error to explain the mismatch; got:\n{msg}"
    )


@given(
    "a scenario body file containing well-formed Gherkin steps",
    target_fixture="bc_root_and_body_path",
)
def given_scenario_body_file_for_cli(tmp_path: Path) -> tuple[Path, Path]:
    bc_root = tmp_path / "bc"
    (bc_root / "inbox").mkdir(parents=True)
    (bc_root / "outbox").mkdir()
    body = (
        "Scenario: hash-matches-body round-trip\n"
        "    Given a well-formed scenario body\n"
        "    When I send it through shop-msg send assign_scenarios\n"
        "    Then the resulting payload validates against the schema\n"
    )
    body_path = tmp_path / "body.txt"
    body_path.write_text(body)
    return bc_root, body_path


@when(
    parsers.parse(
        'I invoke "{cli_phrase}" with that scenario file'
    )
)
def when_invoke_shopmsg_send_assign_scenarios(
    cli_phrase: str, bc_root_and_body_path: tuple[Path, Path], context: dict
) -> None:
    assert cli_phrase == "shop-msg send assign_scenarios", (
        f"this step only handles 'shop-msg send assign_scenarios'; got {cli_phrase!r}"
    )
    bc_root, body_path = bc_root_and_body_path
    result = subprocess.run(
        [
            "shop-msg",
            "send",
            "assign_scenarios",
            "--bc-root",
            str(bc_root),
            "--work-id",
            "lead-018-roundtrip",
            "--feature-title",
            "hash matches body round-trip",
            "--bc-tag",
            "shop-msg",
            "--scenario-file",
            str(body_path),
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr
    context["bc_root_roundtrip"] = bc_root


@then("the resulting inbox YAML deserializes into an AssignScenarios message")
def then_inbox_yaml_deserializes(context: dict) -> None:
    rc = context["cli_returncode"]
    assert rc == 0, (
        f"shop-msg exited {rc}; stderr:\n{context['cli_stderr']}"
    )
    bc_root: Path = context["bc_root_roundtrip"]
    # Read the inbox row from DB instead of the file system.
    raw = read_inbox_message(str(bc_root.resolve()), "lead-018-roundtrip")
    assert raw is not None, (
        f"expected inbox row for work_id='lead-018-roundtrip'; "
        f"inbox rows: {_fetch_inbox_rows(bc_root)}"
    )
    msg = AssignScenarios(**raw)
    context["roundtrip_message"] = msg


@then(
    "each ScenarioPayload in that message satisfies the schema-level "
    "hash-matches-body invariant"
)
def then_each_payload_satisfies_invariant(context: dict) -> None:
    msg: AssignScenarios = context["roundtrip_message"]
    assert msg.scenarios, "expected at least one scenario in the round-trip message"
    for sp in msg.scenarios:
        expected = _scenario_hash_via_cli(sp.gherkin)
        assert sp.hash == expected, (
            f"round-trip payload violates hash↔body invariant: "
            f"hash={sp.hash!r}, canonical(gherkin)={expected!r}"
        )


@when(
    parsers.re(
        r'I run shop-msg respond mechanism_observation with work-id '
        r'"(?P<work_id>[^"]*)" and subject "(?P<subject>[^"]*)" and '
        r'body "(?P<body>[^"]*)"'
    )
)
def run_respond_mechanism_observation(
    bc_root: Path, work_id: str, subject: str, body: str, context: dict
) -> None:
    result = subprocess.run(
        [
            "shop-msg", "respond", "mechanism_observation",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--subject", subject,
            "--body", body,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then(
    parsers.re(
        r'the file parses as a valid MechanismObservation with '
        r'work_id "(?P<work_id>[^"]*)" and subject "(?P<subject>[^"]*)"'
    )
)
def file_parses_as_mechanism_observation(
    bc_root: Path, work_id: str, subject: str, context: dict
) -> None:
    # Read from DB: outbox row for work_id with message_type=mechanism_observation.
    payload = _fetch_outbox_payload(bc_root, work_id, "mechanism_observation")
    assert payload is not None, (
        f"expected outbox row for work_id={work_id!r} "
        f"message_type='mechanism_observation'"
    )
    obs = MechanismObservation.model_validate(payload)
    assert obs.subject == subject


# -----------------------------------------------------------------------
# lead-231.1: pending enumeration and read inbox
# -----------------------------------------------------------------------


def _shop_msg_send_inbox(bc_root: Path, message_type: str, work_id: str) -> None:
    """Drive `shop-msg send <message_type>` for the pending-listing tests."""
    if message_type == "request_maintenance":
        cmd = [
            "shop-msg", "send", "request_maintenance",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--description", "pending-listing setup payload",
        ]
    elif message_type == "request_bugfix":
        cmd = [
            "shop-msg", "send", "request_bugfix",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--description", "pending-listing setup payload",
        ]
    elif message_type == "assign_scenarios":
        body_path = bc_root.parent / f"_pending_body_{work_id}.txt"
        body_path.write_text(
            "Scenario: pending-listing setup\n"
            "    Given an inbox setup body\n"
            "    When the BC receives it\n"
            "    Then it is parsed as a valid AssignScenarios\n"
        )
        cmd = [
            "shop-msg", "send", "assign_scenarios",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--feature-title", "pending-listing setup",
            "--bc-tag", "shopsystem-messaging",
            "--scenario-file", str(body_path),
        ]
    else:
        raise AssertionError(f"unhandled message_type in setup helper: {message_type!r}")
    subprocess.run(cmd, capture_output=True, text=True, check=True)


@given(
    parsers.parse(
        'shop-msg send assign_scenarios was previously used to write an '
        'inbox message with work-id "{work_id}"'
    )
)
def given_prior_inbox_assign_scenarios(bc_root: Path, work_id: str) -> None:
    _shop_msg_send_inbox(bc_root, "assign_scenarios", work_id)


@given(
    parsers.parse(
        'shop-msg send request_bugfix was previously used to write an '
        'inbox message with work-id "{work_id}"'
    )
)
def given_prior_inbox_request_bugfix(bc_root: Path, work_id: str) -> None:
    _shop_msg_send_inbox(bc_root, "request_bugfix", work_id)


@given(
    parsers.parse(
        'shop-msg send request_maintenance was previously used to write an '
        'inbox message with work-id "{work_id}"'
    )
)
def given_prior_inbox_request_maintenance(bc_root: Path, work_id: str) -> None:
    _shop_msg_send_inbox(bc_root, "request_maintenance", work_id)


@given(
    parsers.parse(
        'shop-msg respond work_done was previously used to write an '
        'outbox response with work-id "{work_id}" and status "{status}"'
    )
)
def given_prior_outbox_work_done(bc_root: Path, work_id: str, status: str) -> None:
    subprocess.run(
        [
            "shop-msg", "respond", "work_done",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--status", status,
        ],
        capture_output=True,
        text=True,
        check=True,
    )


@given(
    parsers.parse(
        'shop-msg send assign_scenarios was previously used to write an '
        'inbox message with work-id "{work_id}" containing one '
        'ScenarioPayload tagged "{bc_tag}"'
    )
)
def given_prior_inbox_assign_scenarios_tagged(
    bc_root: Path, work_id: str, bc_tag: str, context: dict
) -> None:
    suffix = bc_tag.removeprefix("@bc:") if bc_tag.startswith("@bc:") else bc_tag
    body_path = bc_root.parent / f"_read_body_{work_id}.txt"
    body_text = (
        "Scenario: read-inbox happy-path setup\n"
        "    Given a tagged scenario body\n"
        "    When the BC reads its inbox\n"
        "    Then the gherkin body is visible in stdout\n"
    )
    body_path.write_text(body_text)
    subprocess.run(
        [
            "shop-msg", "send", "assign_scenarios",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--feature-title", "read-inbox happy-path setup",
            "--bc-tag", suffix,
            "--scenario-file", str(body_path),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    context["read_inbox_body_text"] = body_text


@given(
    parsers.parse(
        'the BC\'s inbox already contains a file for work-id "{work_id}" '
        'whose content is valid YAML but does not match any LeadMessage schema'
    )
)
def given_inbox_invalid_lead_message(
    bc_root: Path, work_id: str
) -> None:
    # Insert a payload that is valid JSON but fails the LeadMessage schema.
    invalid_payload = {
        "message_type": "not_a_real_type",
        "work_id": work_id,
        "description": "this payload is structurally valid YAML",
    }
    insert_raw_payload(
        str(bc_root.resolve()),
        work_id,
        "inbox",
        "not_a_real_type",
        invalid_payload,
    )


@given(
    parsers.parse(
        'a lead shop at a temporary path with BC clones "{bc_a}" and '
        '"{bc_b}" present as sibling directories'
    ),
    target_fixture="lead_root",
)
def given_lead_shop_with_two_bcs(
    tmp_path: Path, bc_a: str, bc_b: str
) -> Path:
    # Lead-side layout mirrors the production shape: a `repos/` dir
    # holds sibling BC clones, each with its own inbox/outbox dirs.
    # With Postgres storage the dirs are only needed for the bc_root
    # path concept (the CLI resolves bc_root to build the bc identifier).
    lead_root = tmp_path / "lead"
    repos = lead_root / "repos"
    repos.mkdir(parents=True)
    for name in (bc_a, bc_b):
        bc = repos / name
        (bc / "inbox").mkdir(parents=True)
        (bc / "outbox").mkdir()
    return lead_root


@given(
    parsers.parse(
        'shop-msg respond work_done was previously used inside "{bc}" to '
        'write an outbox response with work-id "{work_id}" and status "{status}"'
    )
)
def given_prior_outbox_work_done_in_bc(
    lead_root: Path, bc: str, work_id: str, status: str
) -> None:
    bc_root = lead_root / "repos" / bc
    subprocess.run(
        [
            "shop-msg", "respond", "work_done",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--status", status,
        ],
        capture_output=True,
        text=True,
        check=True,
    )


@given(
    parsers.parse(
        'shop-msg respond clarify was previously used inside "{bc}" to '
        'write an outbox response with work-id "{work_id}" and question "{question}"'
    )
)
def given_prior_outbox_clarify_in_bc(
    lead_root: Path, bc: str, work_id: str, question: str
) -> None:
    bc_root = lead_root / "repos" / bc
    subprocess.run(
        [
            "shop-msg", "respond", "clarify",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
            "--question", question,
        ],
        capture_output=True,
        text=True,
        check=True,
    )


@when(
    "I run the shop-msg subcommand that enumerates pending unprocessed "
    "inbox messages, with no filter"
)
def run_pending_inbox(bc_root: Path, context: dict) -> None:
    result = subprocess.run(
        [
            "shop-msg", "pending", "inbox",
            "--bc-root", str(bc_root),
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr
    context["cli_argv"] = ["shop-msg", "pending", "inbox", "--bc-root", str(bc_root)]


@when(
    parsers.parse(
        'I run the shop-msg subcommand that enumerates pending '
        'unprocessed outbox responses, filtered to BC "{bc}"'
    )
)
def run_pending_outbox_filtered(lead_root: Path, bc: str, context: dict) -> None:
    result = subprocess.run(
        [
            "shop-msg", "pending", "outbox",
            "--lead-root", str(lead_root),
            "--bc", bc,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@when(
    parsers.re(
        r'I run shop-msg read inbox with work-id "(?P<work_id>[^"]*)"$'
    )
)
def run_read_inbox(bc_root: Path, work_id: str, context: dict) -> None:
    result = subprocess.run(
        [
            "shop-msg", "read", "inbox",
            "--bc-root", str(bc_root),
            "--work-id", work_id,
        ],
        capture_output=True,
        text=True,
    )
    context["cli_returncode"] = result.returncode
    context["cli_stdout"] = result.stdout
    context["cli_stderr"] = result.stderr


@then("stdout contains no work_id entries")
def stdout_no_work_id_entries(context: dict) -> None:
    stdout = context.get("cli_stdout", "")
    nonblank = [line for line in stdout.splitlines() if line.strip()]
    assert nonblank == [], (
        f"expected no work_id entries; got lines:\n{nonblank}"
    )


@then("the command did not require the caller to inspect the inbox or outbox directories")
def command_did_not_require_caller_directory_inspection(context: dict) -> None:
    assert "cli_returncode" in context, (
        "expected the pending-inbox subcommand to have been invoked; "
        "the When-step did not record a cli_returncode"
    )
    argv = context.get("cli_argv", [])
    for arg in argv:
        assert "/inbox/" not in arg and not arg.endswith("/inbox"), (
            f"expected argv to not point at inbox/; got {arg!r}"
        )
        assert "/outbox/" not in arg and not arg.endswith("/outbox"), (
            f"expected argv to not point at outbox/; got {arg!r}"
        )


def _stdout_lines(context: dict) -> list[str]:
    return [
        line for line in context.get("cli_stdout", "").splitlines() if line.strip()
    ]


@then(
    parsers.re(
        r'stdout includes an entry for work_id "(?P<work_id>[^"]*)" '
        r'with message_type "(?P<message_type>[^"]*)"$'
    )
)
def stdout_includes_pending_entry(
    context: dict, work_id: str, message_type: str
) -> None:
    for line in _stdout_lines(context):
        tokens = line.split()
        if work_id in tokens and message_type in tokens:
            return
    raise AssertionError(
        f"expected an entry matching work_id={work_id!r} and "
        f"message_type={message_type!r}; lines:\n{_stdout_lines(context)}"
    )


@then(
    parsers.parse(
        'stdout contains no entry for work_id "{work_id}"'
    )
)
def stdout_no_entry_for_work_id(context: dict, work_id: str) -> None:
    for line in _stdout_lines(context):
        tokens = line.split()
        assert work_id not in tokens, (
            f"expected no entry for {work_id!r}; found line: {line!r}"
        )


@then(
    parsers.re(
        r'stdout includes an entry for work_id "(?P<work_id>[^"]*)" '
        r'with message_type "(?P<message_type>[^"]*)" originating from '
        r'BC "(?P<bc>[^"]*)"$'
    )
)
def stdout_includes_pending_outbox_entry(
    context: dict, work_id: str, message_type: str, bc: str
) -> None:
    for line in _stdout_lines(context):
        tokens = line.split()
        if work_id in tokens and message_type in tokens and bc in tokens:
            return
    raise AssertionError(
        f"expected an entry matching work_id={work_id!r}, "
        f"message_type={message_type!r}, bc={bc!r}; lines:\n"
        f"{_stdout_lines(context)}"
    )


@then("stdout includes the gherkin body of the ScenarioPayload that was sent")
def stdout_includes_gherkin_body(context: dict) -> None:
    body_text = context.get("read_inbox_body_text")
    assert body_text is not None, (
        "expected the setup step to have captured the body it sent; "
        "no read_inbox_body_text in context"
    )
    first_body_line = next(
        (l for l in body_text.splitlines() if l.strip()), ""
    )
    stdout = context.get("cli_stdout", "")
    assert first_body_line in stdout, (
        f"expected stdout to contain the body's first non-blank line "
        f"{first_body_line!r}; stdout:\n{stdout}"
    )


@then("stderr explains no inbox message was found for that work_id")
def stderr_explains_no_inbox_message(context: dict) -> None:
    stderr = context.get("cli_stderr", "")
    assert "no inbox message" in stderr, (
        f"expected stderr to explain no inbox message was found; got:\n{stderr}"
    )


# -----------------------------------------------------------------------
# lead-231.2: catalog schema bd-decoupling
# -----------------------------------------------------------------------

_MINIMAL_REQUIRED_PAYLOADS: dict[str, dict] = {
    "AssignScenarios": {
        "message_type": "assign_scenarios",
        "work_id": "lead-bd-decoupling",
        "scenarios": [],
    },
    "RequestBugfix": {
        "message_type": "request_bugfix",
        "work_id": "lead-bd-decoupling",
        "description": "minimal fixture description",
    },
    "RequestMaintenance": {
        "message_type": "request_maintenance",
        "work_id": "lead-bd-decoupling",
        "description": "minimal fixture description",
    },
    "Clarify": {
        "message_type": "clarify",
        "work_id": "lead-bd-decoupling",
        "question": "minimal fixture question",
    },
    "WorkDone": {
        "message_type": "work_done",
        "work_id": "lead-bd-decoupling",
        "status": "complete",
    },
    "MechanismObservation": {
        "message_type": "mechanism_observation",
        "subject": "minimal subject",
        "body": (
            "Minimal mechanism observation body text padded to the "
            "schema-required minimum of fifty characters."
        ),
    },
}


def _schema_class_for(name: str):
    from catalog import schemas as _catalog_schemas

    cls = getattr(_catalog_schemas, name, None)
    assert cls is not None, (
        f"unknown schema class name {name!r}; expected one of "
        f"{sorted(_MINIMAL_REQUIRED_PAYLOADS.keys())}"
    )
    return cls


@given(
    parsers.parse("the {schema_name} schema from the shop-msg catalog"),
    target_fixture="bd_decoupling_schema_name",
)
def given_schema_from_catalog(schema_name: str) -> str:
    assert schema_name in _MINIMAL_REQUIRED_PAYLOADS, (
        f"{schema_name!r} is not one of the six bd-decoupled schemas; "
        f"expected one of {sorted(_MINIMAL_REQUIRED_PAYLOADS.keys())}"
    )
    return schema_name


@when(
    parsers.re(
        r'I construct (?:an?|a) (?P<schema_name>[A-Za-z]+) instance '
        r'supplying only the fields the schema marks as required, '
        r'with no field whose name begins with "bd_" or otherwise '
        r'references a beads issue identifier'
    )
)
def when_construct_minimal_instance(
    schema_name: str,
    bd_decoupling_schema_name: str,
    context: dict,
) -> None:
    assert schema_name == bd_decoupling_schema_name, (
        f"When-step schema {schema_name!r} does not match "
        f"Given-step schema {bd_decoupling_schema_name!r}"
    )
    cls = _schema_class_for(schema_name)
    payload = _MINIMAL_REQUIRED_PAYLOADS[schema_name]
    for key in payload:
        assert not key.startswith("bd_"), (
            f"fixture payload for {schema_name} introduced key {key!r} "
            f"that begins with 'bd_'; that would defeat the test"
        )
        assert "beads" not in key.lower(), (
            f"fixture payload for {schema_name} introduced key {key!r} "
            f"naming beads; that would defeat the test"
        )
    try:
        instance = cls(**payload)
    except Exception as exc:  # pragma: no cover
        context["bd_decoupling_error"] = exc
        context["bd_decoupling_instance"] = None
        return
    context["bd_decoupling_error"] = None
    context["bd_decoupling_instance"] = instance


@then("construction succeeds")
def then_construction_succeeds_bd(context: dict) -> None:
    err = context.get("bd_decoupling_error")
    assert err is None, (
        f"expected construction to succeed; got {type(err).__name__}: {err}"
    )
    assert context.get("bd_decoupling_instance") is not None, (
        "expected an instance to be constructed; got None"
    )


@then("no schema validation error is raised")
def then_no_validation_error(context: dict) -> None:
    err = context.get("bd_decoupling_error")
    assert err is None, (
        f"expected no validation error; got {type(err).__name__}: {err}"
    )


@then(
    "no required field of the schema names a beads identifier "
    "in its name, type, or validation pattern"
)
def then_no_required_field_names_beads(
    bd_decoupling_schema_name: str,
) -> None:
    cls = _schema_class_for(bd_decoupling_schema_name)
    for name, field in cls.model_fields.items():
        if not field.is_required():
            continue
        assert not name.startswith("bd_"), (
            f"{cls.__name__}.{name} is required and begins with 'bd_'; "
            f"violates lead-231 item C decoupling invariant"
        )
        assert "beads" not in name.lower(), (
            f"{cls.__name__}.{name} is required and names beads; "
            f"violates lead-231 item C decoupling invariant"
        )
        annotation_str = str(field.annotation).lower()
        assert "beads" not in annotation_str, (
            f"{cls.__name__}.{name} has annotation {field.annotation!r} "
            f"that names beads; violates lead-231 item C"
        )
        for meta in getattr(field, "metadata", []):
            pattern = getattr(meta, "pattern", None)
            if pattern is None:
                continue
            assert "-[a-z0-9]+$" not in pattern, (
                f"{cls.__name__}.{name} validation pattern {pattern!r} "
                f"matches the beads issue-id shape; violates lead-231 "
                f"item C decoupling invariant"
            )
