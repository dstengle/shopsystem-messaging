"""BC-side authoritative scenario-completion journal and lead-side
reconciliation of a snapshot against it (lead-9b3w).

A Bounded Context records the completion of a scenario by appending the
scenario's *block-only canonical hash* (the same hash that lives on the
``scenarios[].hash`` wire field and the on-disk ``@scenario_hash:`` tag —
see ADR-019 / `catalog.schemas`) to an authoritative, append-only journal.

Two domain objects live here:

- ``ScenarioJournal`` — a BC's own append-only record of the block-only
  canonical hashes it has completed. Appending a hash that is already
  journaled is a no-op; an append never removes or overwrites a
  previously-journaled hash. This is the BC's source of truth for "which
  scenarios have I demonstrated PINNED & DEMONSTRATED."

- ``LeadSnapshot`` — the lead's view of which hashes each BC has
  completed. The lead does not write to a BC's journal; it *pulls* the
  journal on demand and reconciles its own per-BC snapshot against it.
  After reconciliation the snapshot records every hash the journal
  carries for that BC, entry for entry.

The journal is keyed purely on the block-only canonical hash string. It
deliberately carries no gherkin, no Feature header, no timestamps — only
the hashes — because the hash is the stable identity the lead reconciles
against (the same identity the BC echoes in ``work_done.scenario_hashes``).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Set, Union

# A committed @scenario_hash tag on a feature block: the tag value is the
# block-only canonical hash for that scenario block (ADR-019). The rebuild
# below treats the *committed tag value* as authoritative — it is the
# "as-committed @scenario_hash tag" the journal bootstraps from.
_SCENARIO_HASH_TAG = re.compile(r"@scenario_hash:([0-9a-fA-F]+)")


class ScenarioJournal:
    """A BC's authoritative, append-only journal of completed scenario
    block-only canonical hashes.

    Invariants:

    * ``append`` adds a hash exactly once. Re-appending an already-present
      hash is a no-op (idempotent), so the journal never contains a hash
      twice.
    * ``append`` never removes or overwrites any previously-journaled
      hash; existing entries and their relative order are preserved.
    """

    def __init__(self) -> None:
        self._entries: List[str] = []

    def contains(self, scenario_hash: str) -> bool:
        """True if ``scenario_hash`` has already been journaled."""
        return scenario_hash in self._entries

    def append(self, scenario_hash: str) -> None:
        """Append ``scenario_hash`` as a new journal entry on completion.

        Idempotent: if the hash is already journaled this is a no-op. The
        append never removes or overwrites a previously-journaled hash.
        """
        if scenario_hash in self._entries:
            return
        self._entries.append(scenario_hash)

    def entries(self) -> List[str]:
        """Return the journaled hashes in append order (a copy)."""
        return list(self._entries)

    def rebuild_from_features(self, features_tree: Union[str, Path]) -> None:
        """Rebuild this journal from a BC's as-committed features tree.

        Scans ``features_tree`` (a directory) for ``@scenario_hash:<hash>``
        tags on the feature blocks committed there, and BOOTSTRAPS each
        committed tag value into the journal as an entry. The bootstrap
        predicate is the as-committed tag ALONE: a hash present on a
        committed feature block is journaled here even though no
        ``work_done`` event drove an append for it. This is how a BC
        reconstructs its authoritative journal from the gated,
        commit-time-green features tree (the source of truth on disk) when
        the journal does not yet carry those hashes.

        The committed tag value is taken verbatim as the block-only
        canonical hash — the tag the BC writes on disk for a scenario block
        already IS that block's block-only canonical hash (ADR-019), so the
        rebuild reuses it rather than re-deriving it.

        Idempotent and non-destructive: each committed tag is appended via
        :meth:`append`, so rebuilding a second time over the same
        as-committed features tree yields a journal identical entry by
        entry — a hash already journaled is not duplicated, and no
        previously-journaled hash (whether bootstrapped or work_done-driven)
        is removed or overwritten.
        """
        root = Path(features_tree)
        # A stable, deterministic walk order so the bootstrapped entries are
        # appended in a reproducible order across rebuilds.
        for feature_file in sorted(root.rglob("*.feature")):
            text = feature_file.read_text(encoding="utf-8")
            for match in _SCENARIO_HASH_TAG.finditer(text):
                self.append(match.group(1))


class LeadSnapshot:
    """The lead's per-BC view of which scenario block-only canonical
    hashes each BC has completed.

    The snapshot is the lead's own record, distinct from any BC journal.
    The lead keeps it current by pulling a BC's journal on demand and
    reconciling: every hash the journal carries for that BC is recorded
    as completed in the snapshot. Reconciliation never invents entries
    the journal does not carry, and after it runs the snapshot matches
    the BC's journal for that BC entry by entry.
    """

    def __init__(self) -> None:
        # bc name -> ordered list of completed hashes recorded for it.
        self._completed: Dict[str, List[str]] = {}

    def records_completed(self, bc: str, scenario_hash: str) -> bool:
        """True if the snapshot records ``scenario_hash`` as completed
        for ``bc``."""
        return scenario_hash in self._completed.get(bc, [])

    def completed_for(self, bc: str) -> List[str]:
        """Return the hashes the snapshot records as completed for ``bc``
        (a copy, in recorded order)."""
        return list(self._completed.get(bc, []))

    def reconcile_against(self, bc: str, journal: ScenarioJournal) -> None:
        """Pull ``journal`` on demand and reconcile this snapshot's record
        for ``bc`` against it.

        After this call every hash the journal carries is recorded as
        completed for ``bc`` in this snapshot, in the journal's order, so
        ``completed_for(bc)`` matches ``journal.entries()`` entry by entry.
        """
        self._completed[bc] = journal.entries()

    def apply_work_done(self, bc: str, scenario_hash: str) -> None:
        """Incrementally record ``scenario_hash`` as completed for ``bc``
        from a single arriving ``work_done``.

        This is the incremental counterpart to ``reconcile_against``: it
        touches only the one hash the ``work_done`` carries and only the
        one BC entry it names. It deliberately does NOT pull or sweep any
        BC journal — not the named BC's and not any other BC's — so a
        newly-completed scenario is reflected in the snapshot without the
        cost (and staleness window) of a full reconciliation pass over
        every BC journal. Recording an already-recorded hash is a no-op,
        so repeated delivery of the same ``work_done`` is idempotent.
        """
        recorded = self._completed.setdefault(bc, [])
        if scenario_hash not in recorded:
            recorded.append(scenario_hash)


class CompletionState:
    """A lookup of which scenario block-only canonical hashes are recorded
    as completed.

    The lookup is keyed *purely* on the block-only canonical hash string —
    the same identity that lives on ``scenarios[].hash``, the on-disk
    ``@scenario_hash:`` tag, and ``work_done.scenario_hashes``. It is NOT
    keyed on bead id, scenario title, or any dispatch record; two scenarios
    that share a block-only canonical hash share a completion answer, and a
    hash is the only thing that can be presented to ``is_completed``.
    """

    def __init__(self, completed: Iterable[str] = ()) -> None:
        self._completed: Set[str] = set(completed)

    def record_completed(self, scenario_hash: str) -> None:
        """Record ``scenario_hash`` as a completed scenario."""
        self._completed.add(scenario_hash)

    def is_completed(self, scenario_hash: str) -> bool:
        """Return a definite boolean answer for ``scenario_hash``.

        ``True`` (a definite "yes") iff the hash was recorded as completed;
        ``False`` (a definite "no") otherwise. The answer is a function of
        the hash alone — no bead id, title, or dispatch record participates.
        """
        return scenario_hash in self._completed


class SystemStateView:
    """The lead's system-state view that incorporates BC-journaled
    completions and classifies each against the lead's canonical features.

    The lead's canonical features are the set of block-only canonical
    hashes that appear as ``@scenario_hash`` tags under the lead's
    ``features/``. A BC-journaled completion whose hash is one of those is a
    *recognized* completion and counts toward coverage. A BC-journaled
    completion whose hash is absent from the canonical features is an
    *orphan anomaly*: it is surfaced for investigation and excluded from
    both the coverage count (numerator) and the outstanding denominator,
    because the lead has no canonical scenario it could be covering.
    """

    def __init__(self, canonical_hashes: Iterable[str]) -> None:
        self._canonical: Set[str] = set(canonical_hashes)
        self._completed: Set[str] = set()

    def incorporate_completion(self, scenario_hash: str) -> None:
        """Incorporate a BC-journaled completion for ``scenario_hash``."""
        self._completed.add(scenario_hash)

    def is_orphan(self, scenario_hash: str) -> bool:
        """True if an incorporated completion is an unrecognized orphan
        anomaly: completed per a BC journal but absent from the lead's
        canonical features."""
        return scenario_hash in self._completed and scenario_hash not in self._canonical

    def orphan_anomalies(self) -> Set[str]:
        """Return every incorporated completion that is an orphan anomaly,
        surfaced for investigation."""
        return {h for h in self._completed if h not in self._canonical}

    def coverage_count(self) -> int:
        """The coverage numerator: incorporated completions that map to a
        canonical feature. Orphan completions are excluded."""
        return len(self._completed & self._canonical)

    def outstanding_denominator(self) -> int:
        """The outstanding denominator: the lead's canonical features.

        Orphan completions are excluded — they never join the denominator,
        because they correspond to no canonical scenario the lead is
        tracking coverage for."""
        return len(self._canonical)
