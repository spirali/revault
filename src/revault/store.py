import threading
from contextvars import ContextVar
from typing import Union, Any
from threading import Lock
from dataclasses import dataclass, field

from .comp import Ref, ToKey, to_key
from .database import Database
from .entry import AnnounceResult, EntryId, Entry
from .key import Key


_GLOBAL_STORE: ContextVar[Union[None, "Store"]] = ContextVar(
    "_GLOBAL_STORE", default=None
)


@dataclass
class RunningTask:
    deps: set[EntryId] = field(default_factory=set)


_CURRENT_RUNNING_TASK: ContextVar[Union[None, RunningTask]] = ContextVar(
    "_CURRENT_RUNNING_TASK", default=None
)


class WaitingForResult:
    def __init__(self):
        self.condition = None
        self.result = None
        self.exception = None
        self.entry_id = None

    def wait(self, lock):
        if self.condition is None:
            self.condition = threading.Condition(lock)
        self.condition.wait()
        if self.exception:
            raise self.exception
        return self.result, self.entry_id

    def set_result(self, result, entry_id):
        if self.condition is None:
            return
        self.result = result
        self.entry_id = entry_id
        self.condition.notify_all()

    def set_exception(self, exception):
        if self.condition is None:
            return
        self.exception = exception
        self.condition.notify_all()


class Store:
    """
    Core class of revault.

    It manages database with results

    For SQLite:

    >>> runtime = Store("sqlite:///path/to/dbfile.db")

    For Postgress:

    >>> runtime = Store("postgresql://<USERNAME>:<PASSWORD>@<HOSTNAME>/<DATABASE>")
    """

    def __init__(self, db_path: str):
        self.db = Database(db_path)
        self.db.init()
        self._token = None

        self.lock = Lock()
        self.waiting_for_results: dict[Key, WaitingForResult | None] = {}

    def get(self, ref: Ref) -> Any:
        return self.get_entry(ref).result

    def get_entry(self, ref: Ref):
        if not isinstance(ref, Ref):
            raise Exception(f"Expected Ref, got {ref.__class__.__name__}")

        with self.lock:
            if ref.key in self.waiting_for_results:
                waiting = self.waiting_for_results[ref.key]
                result, entry_id = waiting.wait(self.lock)
                return Entry(entry_id, ref, result)
            status, entry_id, result = self.db.get_or_announce_entry(ref.key)
            if status == AnnounceResult.FINISHED:
                return Entry(entry_id, ref, result)
            elif status == AnnounceResult.COMPUTING_ELSEWHERE:
                raise Exception(f"Computation {ref} is computed in another process")
            assert status == AnnounceResult.COMPUTE_HERE
            waiting = WaitingForResult()
            self.waiting_for_results[ref.key] = waiting
        try:
            running_task = RunningTask()
            token = _CURRENT_RUNNING_TASK.set(running_task)
            result = ref.computation.fn(**ref.args)
            _CURRENT_RUNNING_TASK.reset(token)
        except BaseException as e:
            self.db.cancel_entry(entry_id)
            with self.lock:
                del self.waiting_for_results[ref.key]
                waiting.set_exception(e)
            raise e
        self.db.finish_entry(entry_id, result, {}, ref.key.config)
        with self.lock:
            del self.waiting_for_results[ref.key]
            waiting.set_result(result, entry_id)
        return Entry(entry_id, ref, result)

    def remove(self, key: ToKey):
        key = to_key(key)
        self.db.remove(key)

    def load(self, key: ToKey):
        return self.load_entry(key).result

    def load_or_none(self, key: ToKey):
        entry = self.load_entry_or_none(key)
        if entry:
            return entry.result
        else:
            return None

    def load_entry(self, key: ToKey):
        entry = self.load_entry_or_none(key)
        if entry is None:
            raise Exception(f"Key {to_key(key)} not found.")
        return entry

    def load_replica_entries(self, key: ToKey) -> list:
        key = to_key(key)
        return self.db.load_replica_entries(key)

    def load_replicas(self, key: ToKey) -> list:
        return [entry.result for entry in self.load_replica_entries(key)]

    def load_entry_or_none(self, key: ToKey):
        return self.db.load_entry(to_key(key))

    def insert_new_replica(self, key: ToKey, result) -> Key:
        key = to_key(key)
        replica = self.db.insert_new_replica(key, result)
        return Key(key.name, key.version, key.config, replica, key.config_key)

    # def query(self, name: Computation) -> list[Key]:
    #     return self.db.query_by_name(name)

    def query_keys(self, computation: "Computation") -> list[Key]:
        return self.db.query_keys(computation.name, computation.version)

    def all_keys(self) -> list[Key]:
        return self.db.load_all_keys()

    def cancel_running(self):
        self.db.cancel_running()

    def __enter__(self):
        assert self._token is None
        self._token = _GLOBAL_STORE.set(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        _GLOBAL_STORE.reset(self._token)
        self._token = None


def get_current_store() -> Store:
    runtime = _GLOBAL_STORE.get()
    if runtime is None:
        raise Exception("No default store")
    return runtime


from .comp import Computation  # noqa: E402
