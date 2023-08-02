import typing
import dc_banyan._banyan as _banyan

# Re-exports
ipfs_available = _banyan.ipfs_available
Float = _banyan.Float
Integer = _banyan.Integer
Record = _banyan.PyRecord
String = _banyan.String
Timestamp = _banyan.Timestamp

_PRIVATE = object()


class DataDefinition:
    def __init__(self, columns):
        self._inner = _banyan.PyDataDefinition(columns)

    def record(self):
        return self._inner.record()


class _PrivateWrapper:
    def __init__(self, _private, inner):
        assert _private is _PRIVATE  # prevent user instantiation
        self._inner = inner


class Resolver(_PrivateWrapper):
    def new_datastream(self, data_definition):
        return Datastream(_PRIVATE, self._inner.new_datastream(data_definition._inner))

    def load_datastream(self, data_definition, cid):
        return Datastream(
            _PRIVATE, self._inner.load_datastream(data_definition._inner, cid)
        )


class Datastream(_PrivateWrapper):
    @property
    def cid(self):
        return self._inner.cid

    def extend(self, records):
        return Datastream(_PRIVATE, self._inner.extend(records))

    def collect(self):
        return self._inner.collect()

    def __iter__(self):
        return iter(self.collect())


def ipfs_resolver():
    return Resolver(_PRIVATE, _banyan.ipfs_resolver())


def memory_resolver(max_size: int):
    return Resolver(_PRIVATE, _banyan.memory_resolver(max_size))


def Enum(options: typing.Sequence[str]):
    return list(options)
