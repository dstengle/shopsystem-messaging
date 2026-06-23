"""Per-worker Postgres database isolation under xdist (work_id: lead-ufhs).

The full pytest suite shares a single Postgres database (`shopsystem_test`).
The per-test teardown sweep ``DELETE FROM messages WHERE bc LIKE 'shopsystem/%'``
keeps the suite deterministic under randomized *order* (single process), but it
is catastrophic under *parallelism*: every xdist worker targets the SAME
database, so

  * worker gw1's ``pytest_sessionstart`` DROP/CREATEs the database gw0 is
    actively running against (DROP DATABASE terminates gw0's backends), and
  * worker gw0's per-test ``messages`` sweep deletes worker gw1's in-flight
    rows mid-test.

The fix is per-worker database isolation: each xdist worker gets its own
database name (``shopsystem_test_gw0``, ``shopsystem_test_gw1``, ...) derived
from ``PYTEST_XDIST_WORKER`` so workers never share message-row or registry
state. The non-xdist / controller case keeps the bare base name.

These tests pin the worker-scoped-name derivation seam and assert the live
test DSN the conftest installed targets a worker-scoped database name.
"""
import os

import conftest


def test_worker_scoped_dbname_is_unique_per_worker():
    """Distinct xdist workers map to distinct database names."""
    base = "shopsystem_test"
    gw0 = conftest._worker_scoped_dbname(base, "gw0")
    gw1 = conftest._worker_scoped_dbname(base, "gw1")

    assert gw0 != gw1, "gw0 and gw1 must target distinct databases"
    assert gw0 == "shopsystem_test_gw0"
    assert gw1 == "shopsystem_test_gw1"


def test_worker_scoped_dbname_controller_keeps_base():
    """The controller / non-xdist case (no worker id) keeps the bare base name."""
    base = "shopsystem_test"
    # xdist leaves PYTEST_XDIST_WORKER unset in the controller and in a plain
    # (non-parallel) run; some xdist versions report the controller as "master".
    assert conftest._worker_scoped_dbname(base, None) == base
    assert conftest._worker_scoped_dbname(base, "") == base
    assert conftest._worker_scoped_dbname(base, "master") == base


def test_live_test_dsn_is_worker_scoped():
    """The DSN installed at conftest import targets the current worker's DB.

    Under a plain run PYTEST_XDIST_WORKER is unset and the DB name is the bare
    base; under xdist worker gwN it must carry the ``_gwN`` suffix. In both
    cases the database name embedded in the live ``SHOPMSG_DSN`` must equal the
    worker-scoped name the derivation function computes for the current worker.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    expected_dbname = conftest._worker_scoped_dbname("shopsystem_test", worker_id)

    # The conftest's module-level test DB name must be the worker-scoped one.
    assert conftest.SHOPMSG_TEST_DBNAME == expected_dbname

    # And the live DSN the suite actually connects through must embed it.
    dsn = os.environ["SHOPMSG_DSN"]
    assert dsn.rsplit("/", 1)[-1] == expected_dbname
