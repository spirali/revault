from typing import Any, Callable, Iterable
import inspect

from .key import Key


class Ref:
    def __init__(self, key: Key, computation: "Computation", args: dict):
        self.key = key
        self.computation = computation
        self.args = args

    def __revault_key__(self):
        return self.key.__revault_key__()

    def __repr__(self):
        key = self.key
        a = ", ".join(f"{k}={repr(v)}" for k, v in key.config.items())
        return f"<Ref {key.name}({a}) v={key.version} r={key.replica}>"


ToKey = Key | Ref


def to_key(obj: ToKey) -> Key:
    if isinstance(obj, Ref):
        return obj.key
    if isinstance(obj, Key):
        return obj
    raise Exception(f"Expected Ref or CompRef, got: {obj.__type__}")


class Computation:
    def __init__(self, fn: Callable, name: str, version: int, json_inputs: bool, json_result: bool):
        assert isinstance(fn, Callable)
        self.fn = fn
        self.version = version
        self.fn_signature = inspect.signature(fn)
        self.fn_argspec = inspect.getfullargspec(fn)
        self.json_inputs = json_inputs
        self.json_result = json_result
        self.name = name or fn.__name__

        self.__signature__ = self.fn_signature
        if hasattr(self.fn, "__name__"):
            self.__name__ = self.name

    def __repr__(self):
        return f"<Computation '{self.name}' v={self.version}>"

    def ref_from_key(self, key: Key, ephemeral_args: dict | None = None):
        assert key.name == self.name
        assert key.version == self.version
        args = key.config.copy()
        if ephemeral_args:
            args.update(ephemeral_args)
        return Ref(key, self, args)

    def ref(self, *args, version=None, replica=0, **kwargs) -> Ref:
        if version is None:
            version = self.version
        ba = self.fn_signature.bind(*args, **kwargs)
        ba.apply_defaults()
        args = ba.arguments
        if self.fn_argspec.varkw:
            kwargs = args.pop(self.fn_argspec.varkw, {})
            args.update(kwargs)
        config = {}
        for name in args:
            if not name.startswith("__"):
                config[name] = args[name]
        return Ref(Key(self.name, version, config, replica), self, args)

    def replicas_refs(
        self, replicas: int | Iterable[int], *args, **kwargs
    ) -> list[Ref]:
        if isinstance(replicas, int):
            replicas = range(replicas)
        return [self.ref(replica=n, *args, **kwargs) for n in replicas]

    def replicas(self, replicas: int | Iterable[int], *args, **kwargs) -> list[Any]:
        store = get_current_store()
        return [store.get(ref) for ref in self.replicas_refs(replicas, *args, **kwargs)]

    def load_replicas(self, *args, **kwargs):
        return get_current_store().load_replicas(self.ref(*args, **kwargs))

    def load_entry(self, *args, **kwargs):
        return get_current_store().load_entry(self.ref(*args, **kwargs))

    def load(self, *args, **kwargs):
        return get_current_store().load(self.ref(*args, **kwargs))

    def remove(self, *args, **kwargs):
        return get_current_store().remove(self.ref(*args, **kwargs))

    def load_entry_or_none(self, *args, **kwargs):
        return get_current_store().load_entry_or_none(self.ref(*args, **kwargs))

    def load_or_none(self, *args, **kwargs):
        return get_current_store().load_or_none(self.ref(*args, **kwargs))

    def keys(self) -> list[Key]:
        return get_current_store().query_keys(self)

    def __call__(self, *args, **kwargs):
        return get_current_store().get(self.ref(*args, **kwargs))


def computation(fn=None, *, name: str = None, version: int = 0, json_inputs: bool = False, json_result: bool = False):
    def _helper(fn):
        return Computation(fn, name, version, json_inputs, json_result)

    if fn is not None:
        return _helper(fn)
    else:
        return _helper


from .store import get_current_store  # noqa: E402
