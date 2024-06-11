from hashlib import sha224


class Key:
    def __init__(
        self,
        name: str,
        version: int,
        config: dict,
        replica: int,
        config_key: str | None = None,
    ):
        assert isinstance(name, str)
        assert isinstance(config, dict)
        assert isinstance(version, int)
        assert isinstance(replica, int)

        self.name = name
        self.config = config
        self.version = version
        self.replica = replica
        if config_key is None:
            self.config_key = make_key(self.config)
        else:
            self.config_key = config_key

    def __revault_key__(self):
        return f"{self.name},{self.version},{self.config},{self.replica}"

    @property
    def tuple_key(self) -> tuple:
        return self.name, self.config_key, self.version, self.replica

    def __repr__(self):
        a = ", ".join(f"{k}={repr(v)}" for k, v in self.config.items())
        return f"<Key {self.name}({a}) v={self.version} r={self.replica}>"

    def __eq__(self, other):
        if not isinstance(other, Key):
            return False
        return (
            self.config_key == other.config_key
            and self.name == other.name
            and self.version == other.version
            and self.replica == other.replica
        )

    def __hash__(self):
        return hash(self.tuple_key)


def _is_basic_type(obj) -> bool:
    return (
        isinstance(obj, str)
        or isinstance(obj, int)
        or isinstance(obj, float)
        or obj is None
    )


def _make_key_helper(obj, stream):
    if _is_basic_type(obj):
        stream.append(repr(obj))
    elif isinstance(obj, list) or isinstance(obj, tuple):
        stream.append("[")
        for value in obj:
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("]")
    elif isinstance(obj, dict):
        stream.append("{")
        items = []
        for key in obj:
            if _is_basic_type(key):
                new_key = repr(key)
            else:
                new_key = "~" + make_key(key)
            items.append((new_key, obj[key]))
        for key, value in sorted(items):
            stream.append(key)
            stream.append(":")
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    elif hasattr(obj, "__revault_key__"):
        stream.append("<")
        stream.append(obj.__class__.__name__)
        stream.append(" ")
        stream.append(obj.__revault_key__())
        stream.append(">")
    else:
        raise Exception(
            "Invalid item in config: '{}', type: {}".format(repr(obj), type(obj))
        )


def make_key(config):
    stream = []
    _make_key_helper(config, stream)
    return sha224("".join(stream).encode()).hexdigest()
