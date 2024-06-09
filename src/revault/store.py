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


class Store:
    """
    Core class of revault.

    It manages database with results and starts computations

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

    def get(self, ref: Ref) -> Any:
        return self.get_entry(ref).result

    def get_entry(self, ref: Ref):
        if not isinstance(ref, Ref):
            raise Exception(f"Expected CompRef, got {ref.__class__.__name__}")

        status, entry_id, result = self.db.get_or_announce_entry(ref.key)
        if status == AnnounceResult.FINISHED:
            return Entry(entry_id, ref, result)
        elif status == AnnounceResult.COMPUTE_HERE:
            try:
                running_task = RunningTask()
                token = _CURRENT_RUNNING_TASK.set(running_task)
                result = ref.computation.fn(**ref.args)
                _CURRENT_RUNNING_TASK.reset(token)
            except BaseException as e:
                self.db.cancel_entry(entry_id)
                raise e
            self.db.finish_entry(entry_id, result, {})
            return Entry(entry_id, ref, result)
        elif status == AnnounceResult.COMPUTING_ELSEWHERE:
            raise Exception(f"Computation {ref} is computed in another process")

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

    # def get_entries(self, obj):
    #     refs = collect_refs(obj)
    #     return replace_refs(obj, self._process_refs(refs))

    # def get_results(self, obj):
    #     refs = collect_refs(obj)
    #     results = {ref: entry.result for ref, entry in self._process_refs(refs).items()}
    #     return replace_refs(obj, results)

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


# def get_results(obj):
#     return get_current_store().get_results(obj)


# def read_results(obj):
#     return get_current_store().read_results(obj)


from .comp import Computation
