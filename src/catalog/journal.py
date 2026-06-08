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

from typing import Dict, List


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
