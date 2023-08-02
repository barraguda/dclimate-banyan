import datetime
import pytest

import dc_banyan

SIZE_64_MB = 1 << 26


@pytest.fixture(scope="session")
def data_definition():
    return dc_banyan.DataDefinition(
        (
            ("ts", dc_banyan.Timestamp, True),
            ("one", dc_banyan.Integer, True),
            ("two", dc_banyan.Integer, False),
            ("three", dc_banyan.Float, True),
            ("four", dc_banyan.Float, False),
            ("five", dc_banyan.String, True),
            ("six", dc_banyan.String, False),
            # ("seven", dc_banyan.Enum(("foo", "bar", "baz")), True),
            # ("eight", dc_banyan.Enum(("boo", "far", "faz")), True),
            ("seven", dc_banyan.Enum(("foo", "bar", "baz")), False),
            ("eight", dc_banyan.Enum(("boo", "far", "faz")), False),
            ("nine", dc_banyan.Timestamp, False),
        )
    )


@pytest.fixture(scope="session")
def resolver():
    if dc_banyan.ipfs_available():
        return dc_banyan.ipfs_resolver()

    return dc_banyan.memory_resolver(SIZE_64_MB)


def some_string(i: int):
    ALPHA = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def some_chars(i):
        while i != 0:
            char = i % 52
            yield ALPHA[char]
            i //= 52

    return "".join(some_chars(i))


def make_records(n: int, definition: dc_banyan.DataDefinition):
    seven_values = ("foo", "bar", "baz")
    eight_values = ("boo", "far", "faz")

    records = []
    for i in range(n):
        record = definition.record()
        record["ts"] = datetime.datetime.fromtimestamp(i)
        record["one"] = 100 + i * 3
        if i % 2 == 0:
            record["two"] = i + i * 2
        if i % 3 == 0:
            record["three"] = i / 1.2
        if i % 4 != 2:
            record["four"] = i * 3.141592
        if i % 5 == 0:
            record["five"] = some_string(i)
        if i % 2 == 0:
            record["six"] = some_string(i * 1013)
        record["seven"] = seven_values[i % 3]
        if i % 3 == 0:
            record["eight"] = eight_values[i % 3]
        if i % 5 != 1:
            record["nine"] = datetime.datetime.fromtimestamp(i * 512 - 6)

        records.append(record)

    return records


def test_codec(resolver, data_definition):
    n = 100000
    datastream = resolver.new_datastream(data_definition)
    assert datastream.cid is None
    assert list(datastream) == []

    records = make_records(n, data_definition)
    datastream = datastream.extend(records[: n // 2])
    assert datastream.cid is not None

    datastream = resolver.load_datastream(data_definition, datastream.cid)
    stored = list(datastream)
    assert len(stored) == n // 2

    datastream = datastream.extend(records[n // 2 :])
    stored = list(datastream)
    assert len(stored) == n
    assert records == stored


def test_record___getitem__(data_definition):
    record = make_records(1, data_definition)[0]
    assert record["ts"] == datetime.datetime.fromtimestamp(0)
    assert record["one"] == 100
    assert record["two"] == 0
    assert record["three"] == 0.0
    assert record["six"] == ""
    assert record["seven"] == "foo"


def test_record___delitem__(data_definition):
    record = make_records(1, data_definition)[0]
    assert record["one"] == 100
    del record["one"]
    with pytest.raises(KeyError):
        record["one"]
