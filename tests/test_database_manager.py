import threading

from storage.database.manager import DBManager


def test_close_duckdb_releases_only_duckdb_connection():
    class FakeConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    manager = DBManager.__new__(DBManager)
    manager._duckdb_conn = FakeConnection()
    manager._duckdb_lock = threading.RLock()

    connection = manager._duckdb_conn
    manager.close_duckdb()

    assert connection.closed
    assert manager._duckdb_conn is None
    manager.close_duckdb()


def test_duckdb_lock_serializes_connection_owners():
    manager = DBManager.__new__(DBManager)
    manager._duckdb_lock = threading.RLock()
    entered = threading.Event()
    release = threading.Event()
    waiter_finished = threading.Event()

    def hold_connection_lock():
        with manager.duckdb_lock:
            entered.set()
            release.wait(timeout=1)

    def wait_for_connection_lock():
        with manager.duckdb_lock:
            waiter_finished.set()

    holder = threading.Thread(target=hold_connection_lock)
    waiter = threading.Thread(target=wait_for_connection_lock)
    holder.start()
    assert entered.wait(timeout=1)
    waiter.start()
    assert not waiter_finished.wait(timeout=0.05)
    release.set()
    holder.join(timeout=1)
    waiter.join(timeout=1)
    assert waiter_finished.is_set()
