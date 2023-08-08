import typing
import dc_banyan._banyan as _banyan

# Re-exports
ipfs_available = _banyan.ipfs_available
ipfs_store = _banyan.ipfs_store
memory_store = _banyan.memory_store
Float = _banyan.Float
Integer = _banyan.Integer
Record = _banyan.PyRecord
String = _banyan.String
Timestamp = _banyan.Timestamp

_PRIVATE = object()


def new_datastream(store, data_definition):
    return Datastream(_PRIVATE, _banyan.new_datastream(store, data_definition._inner))


def load_datastream(cid, store, data_definition):
    return Datastream(
        _PRIVATE, _banyan.load_datastream(cid, store, data_definition._inner)
    )


class _PrivateWrapper:
    def __init__(self, _private, inner):
        assert _private is _PRIVATE  # prevent user instantiation
        self._inner = inner


class DataDefinition:
    def __init__(self, columns):
        self._inner = _banyan.PyDataDefinition(columns)

    def record(self):
        return self._inner.record()

    def get_by_name(self, name):
        return self._inner.get_by_name(name)

    __getitem__ = get_by_name

    def __repr__(self):
        return repr(self._inner)


class Datastream(_PrivateWrapper):
    @property
    def cid(self):
        return self._inner.cid

    def extend(self, records):
        return Datastream(_PRIVATE, self._inner.extend(records))

    def collect(self):
        if self.cid is None:
            return ()

        return self._inner.collect()

    def __iter__(self):
        return iter(self.collect())

    def query(self, query):
        return iter(self._inner.query(query))

    def __getitem__(self, index):
        if isinstance(index, slice) and index.step is None:
            return Datastream(_PRIVATE, self._inner.slice(index.start, index.stop))

        elif isinstance(index, int):
            return self._inner.slice(index, index + 1).collect()[0]

        raise NotImplementedError


def Enum(options: typing.Sequence[str]):
    return list(options)
