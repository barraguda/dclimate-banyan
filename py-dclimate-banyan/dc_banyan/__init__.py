"""A library for storing, retreiving, and querying time series data on IPFS.

This library is a wrapper for Rüdiger Klaehn's excellent `Banyan library
<https://github.com/Actyx/banyan>`_, written in Rust. This library provides concrete
implementations for the types needed by Banyan and provides a Python wrapper so that it
can be used from Python.
"""
import typing

import dc_banyan._banyan as _banyan

# Re-exports
ipfs_available = _banyan.ipfs_available
ipfs_store = _banyan.ipfs_store
memory_store = _banyan.memory_store

Query = _banyan.Query
Record = _banyan.Record

_PRIVATE = object()


class _PrivateWrapper:
    def __init__(self, _private, inner):
        assert _private is _PRIVATE  # prevent user instantiation
        self._inner = inner


class Store:
    """Abstract base class for objects which can be used by Banyan to store data in
    IPLD."""


class ColumnType:
    """Specifier for the type of data stored in a column.

    These are used to specifify column definitons as part of a ``DataDefinition``.
    Generally, you should use the constants provided at the module level: ``Integer``,
    ``Float``, ``Timestamp``, ``String``, and ``Enum``. ``Enum``, unlike the others,
    takes an argument which is a list of string values that are the possible values for
    the column.

    .. doctest::
        :pyversion: ~= 3.9

        >>> data_definition = DataDefinition([
        ...     ("ts", Timestamp, True),
        ...     ("one", Integer, False),
        ...     ("two", Float, True),
        ...     ("three", String, False),
        ...     ("four", Enum("foo", "bar", "baz"), False)
        ... ])
        >>> assert data_definition["ts"].type == Timestamp
        >>> assert data_definition["four"].type == Enum("foo", "bar", "baz")
    """

    def __init__(self, value):
        if isinstance(value, list):
            enum_values = value
            value = _banyan.Enum
        else:
            enum_values = None

        self.value = value
        self.enum_values = enum_values

    def _rust_value(self):
        return self.enum_values if self.enum_values is not None else self.value

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.value == other.value and self.enum_values == other.enum_values

        return False


Float = ColumnType(_banyan.Float)
""":class:`ColumnType` instance that indicates data for a column are 64 bit floating
point numbers."""

Integer = ColumnType(_banyan.Integer)
""":class:`ColumnType` instance that indicates data for a column are 64 bit integers."""

String = ColumnType(_banyan.String)
""":class:`ColumnType` instance that indicates data for a column are strings."""

Timestamp = ColumnType(_banyan.Timestamp)
""":class:`ColumnType` instance that indicates data for a column are datetime instances.
``datetime.datetime`` from the standard library is used. Values are naive, meaning they
have no timezone associated with them."""


def Enum(*options: str) -> ColumnType:
    """Generate a :class:`ColumnType` where data may be any one of the passed in string
    options."""
    return ColumnType(list(options))


class ColumnDefinition(_PrivateWrapper):
    """The definition for a column in a :class:`DataDefinition`.

    Column definitions may be compared to values of their declared types to create
    :class:`Query` objects suitable for passing to :meth:`Datastream.query`.

    .. doctest::
        :pyversion: ~= 3.9

        >>> year_is_1984 = (
        ...     (data_definition["ts"] >= datetime(1984, 1, 1, 0, 0)) &
        ...     (data_definition["ts"] < datetime(1985, 1, 1, 0, 0))
        ... )
        >>> year_is_1984
        Query { clauses: [QueryAnd { clauses: [QueryCol { operator: GE, position: 0, value: Timestamp(441763200) }, QueryCol { operator: LT, position: 0, value: Timestamp(473385600) }] }] }
    """

    @property
    def name(self) -> str:
        """Name of the column."""
        return self._inner.name

    @property
    def type(self) -> ColumnType:
        """The type of the column."""
        return ColumnType(self._inner.type)

    @property
    def index(self) -> bool:
        """Whether the column is indexed. Only indexed columns may be queried."""
        return self._inner.index

    def __eq__(self, other):
        return self._inner.__eq__(other)

    def __ne__(self, other):
        return self._inner.__ne__(other)

    def __lt__(self, other):
        return self._inner.__lt__(other)

    def __le__(self, other):
        return self._inner.__le__(other)

    def __gt__(self, other):
        return self._inner.__gt__(other)

    def __ge__(self, other):
        return self._inner.__ge__(other)

    def __repr__(self):
        return self._inner.__repr__()


class DataDefinition:
    """Defines the data to be stored in a Datastream.

    The data is defined as a sequence of column definitions. A ``DataDefinition`` can be
    instantiated from a sequence of column definitions passed in as tuples of ``(name:
    str, type: ColumnType, index: bool)``. ``name`` indicates the name of the column in
    the datastream, ``type`` indicates the type of data to be stored (ints, floats,
    etc...), and ``index`` indicates whether or not the column is included in the index
    for the datastream. A column may be used queries if and only if it is indexed.

    .. doctest::
        :pyversion: ~= 3.9

        >>> from dc_banyan import (
        ...     DataDefinition,
        ...     Integer,
        ...     Float,
        ...     String,
        ...     Timestamp,
        ...     Enum,
        ... )
        >>> data_definition = DataDefinition([
        ...     ("ts", Timestamp, True),
        ...     ("one", Integer, False),
        ...     ("two", Float, True),
        ...     ("three", String, False),
        ...     ("four", Enum("foo", "bar", "baz"), False)
        ... ])
        >>> data_definition
        DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "one", kind: Integer, index: false }, ColumnDefinition { position: 2, name: "two", kind: Float, index: true }, ColumnDefinition { position: 3, name: "three", kind: String, index: false }, ColumnDefinition { position: 4, name: "four", kind: Enum(["foo", "bar", "baz"]), index: false }])

    Column definitions can be accessed using :meth:`DataDefinition.get_by_name` or by
    using item access.

    .. doctest::
        :pyversion: ~= 3.9

        >>> data_definition["two"]
        ColumnDefinition { position: 2, name: "two", kind: Float, index: true }

    As an iterable, :class:`DataDefinition` will iterate over it's column definitions.

    .. doctest::
        :pyversion: ~= 3.9

        >>> next(iter(data_definition))
        ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }
    """

    def __init__(self, columns):
        columns = [(name, kind._rust_value(), index) for (name, kind, index) in columns]
        self._inner = _banyan.PyDataDefinition(columns)

    def record(self, values: typing.Dict[str, typing.Any] = None) -> Record:
        """Create a new record with this data definition.

        A record is essentially a dict-like object backed and validated by the columns
        defined in the data definition.

        If ``values`` is not passed in, will instantiate an empty record. Values can be
        added or modified using item assignment.

        .. doctest::
            :pyversion: ~= 3.9

            >>> from datetime import datetime
            >>> record = data_definition.record({
            ...     "ts": datetime(2023, 12, 25, 0, 0, 0),
            ...     "one": 42,
            ... })
            >>> record["ts"]
            datetime.datetime(2023, 12, 25, 0, 0)
            >>> record["two"] = 3.141592
            >>> record["two"]
            3.141592
        """
        if isinstance(values, _banyan.Record):
            return values

        record = self._inner.record()
        if values is not None:
            for k, v in values.items():
                record[k] = v

        return record

    def get_by_name(self, name: str) -> ColumnDefinition:
        """Retrieve a column definition by name."""
        return ColumnDefinition(_PRIVATE, self._inner.get_by_name(name))

    __getitem__ = get_by_name

    def __repr__(self):
        return repr(self._inner)

    def columns(self) -> typing.List[ColumnDefinition]:
        """Get the column definitions for this data definition."""
        return [ColumnDefinition(_PRIVATE, col) for col in self._inner.columns()]

    def __iter__(self):
        return iter(self.columns())


class Datastream(_PrivateWrapper):
    """A datastream, stored using Banyan.

    Datastreams represent a potentially massive stream of time series data, presenting
    an array-like interface as though the data were loaded in memory. Accessing an index
    or a slice, or running a query will cause data to be marshalled from the back-end.

    .. testsetup::

        from datetime import timedelta

        def make_records():
            records = []
            for i in range(1000):
                records.append(data_definition.record({
                    "ts": datetime(2000, 1, 1, 0, 0) + timedelta(days=i), "one": i,
                    "two": i * 3.141592,
                }))

            return records

    To create a new datastream, use :func:`new_datastream`. Data can be appended to the
    end of a datastream using :meth:`Datastream.extend`.

    .. doctest::
        :pyversion: ~= 3.9

        >>> from dc_banyan import (
        ...     memory_store,
        ...     new_datastream,
        ... )
        >>> one_megabyte = 1<<20
        >>> store = memory_store(one_megabyte)
        >>> datastream = new_datastream(store, data_definition)
        >>> records = make_records()
        >>> datastream = datastream.extend(records)  # Creates new datastream

    When you have added data to a datastream, you'll want to save it's unique content
    identifier (CID) for later retrieval.

    .. doctest::
        :pyversion: ~= 3.9

        >>> cid = datastream.cid
        >>> cid
        'bafyreiclr5fo2lr7aeaspa56sjt4s3mhc7nndvdqqcfq7mbkcqjv5chwli'

    You can load a datastream by CID using :func:`load_datastream`.

    .. doctest::
        :pyversion: ~= 3.9

        >>> from dc_banyan import load_datastream
        >>> datastream = load_datastream(cid, store, data_definition)

    Indexing can be used get a single data point.

    .. doctest::
        :pyversion: ~= 3.9

        >>> datastream[0].as_dict()
        {'one': 0, 'ts': datetime.datetime(2000, 1, 1, 0, 0), 'two': 0.0}
        >>> datastream[42].as_dict()
        {'one': 42, 'ts': datetime.datetime(2000, 2, 12, 0, 0), 'two': 131.946864}

    Using a slice is more efficient, though. Slices are lazy. Data isn't retreived until
    you access the data.

    .. doctest::
        :pyversion: ~= 3.9

        >>> records = datastream[100:110]
        >>> records[2].as_dict()
        {'one': 102, 'ts': datetime.datetime(2000, 4, 12, 0, 0), 'two': 320.442384}
        >>> [record.as_dict() for record in datastream[100:110]]
        [{'one': 100, 'ts': datetime.datetime(2000, 4, 10, 0, 0), 'two': 314.1592}, {'one': 101, 'ts': datetime.datetime(2000, 4, 11, 0, 0), 'two': 317.300792}, {'one': 102, 'ts': datetime.datetime(2000, 4, 12, 0, 0), 'two': 320.442384}, {'one': 103, 'ts': datetime.datetime(2000, 4, 13, 0, 0), 'two': 323.583976}, {'one': 104, 'ts': datetime.datetime(2000, 4, 14, 0, 0), 'two': 326.725568}, {'one': 105, 'ts': datetime.datetime(2000, 4, 15, 0, 0), 'two': 329.86716}, {'one': 106, 'ts': datetime.datetime(2000, 4, 16, 0, 0), 'two': 333.008752}, {'one': 107, 'ts': datetime.datetime(2000, 4, 17, 0, 0), 'two': 336.150344}, {'one': 108, 'ts': datetime.datetime(2000, 4, 18, 0, 0), 'two': 339.291936}, {'one': 109, 'ts': datetime.datetime(2000, 4, 19, 0, 0), 'two': 342.433528}]

    You can also query a dataset using :meth:`Dataset.query`.

    .. doctest::
        :pyversion: ~= 3.9

        >>> ts_col = data_definition["ts"]
        >>> dec_2001 = (
        ...     (ts_col >= datetime(2001, 12, 1, 0, 0)) &
        ...     (ts_col < datetime(2002, 1, 1, 0, 0))
        ... )
        >>> query = dec_2001 & (data_definition["two"] > 2290.0)
        >>> results = datastream.query(query)
        >>> [record.as_dict() for record in results]
        [{'one': 729, 'ts': datetime.datetime(2001, 12, 30, 0, 0), 'two': 2290.220568}, {'one': 730, 'ts': datetime.datetime(2001, 12, 31, 0, 0), 'two': 2293.36216}]
    """

    @property
    def cid(self) -> str:
        """The unique content identifier for the datastream."""
        return self._inner.cid

    def extend(self, records: typing.List[Record]) -> "Datastream":
        """Add records to a datastream. Will return a new datastream."""
        return Datastream(_PRIVATE, self._inner.extend(records))

    def collect(self) -> typing.Iterator[Record]:
        """Retrieves all data in the datastream (or slice) and returns as a list.

        Don't be fooled by the returned iterator. All results are retreived before this
        function returns. It is recommended to take a slice of a datastream first and
        then call this on that slice, unless you think the entire dataset will fit in
        RAM.
        """
        if self.cid is None:
            return ()

        return self._inner.collect()

    def __iter__(self):
        return iter(self.collect())

    def query(self, query: Query) -> typing.Iterator[Record]:
        """Runs the given query and returns an iterator over the results.

        Don't be fooled by the returned iterator. All results are retreived before this
        function returns, so beware of large result sets.
        """
        return iter(self._inner.query(query))

    def __getitem__(self, index):
        if isinstance(index, slice) and index.step is None:
            return Datastream(_PRIVATE, self._inner.slice(index.start, index.stop))

        elif isinstance(index, int):
            return self._inner.slice(index, index + 1).collect()[0]

        raise NotImplementedError


def new_datastream(store: Store, data_definition: DataDefinition) -> Datastream:
    return Datastream(_PRIVATE, _banyan.new_datastream(store, data_definition._inner))


new_datastream.__doc__ = _banyan.new_datastream.__doc__


def load_datastream(
    cid: str, store: Store, data_definition: DataDefinition
) -> Datastream:
    return Datastream(
        _PRIVATE, _banyan.load_datastream(cid, store, data_definition._inner)
    )


load_datastream.__doc__ = _banyan.load_datastream.__doc__
