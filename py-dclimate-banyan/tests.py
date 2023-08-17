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
            # ("seven", dc_banyan.Enum("foo", "bar", "baz"), True),
            # ("eight", dc_banyan.Enum("boo", "far", "faz"), True),
            ("seven", dc_banyan.Enum("foo", "bar", "baz"), False),
            ("eight", dc_banyan.Enum("boo", "far", "faz"), False),
            ("nine", dc_banyan.Timestamp, False),
        )
    )


@pytest.fixture(scope="session")
def store():
    if dc_banyan.ipfs_available():
        return dc_banyan.ipfs_store()

    return dc_banyan.memory_store(SIZE_64_MB)


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


def test_codec(store, data_definition):
    n = 10000
    datastream = dc_banyan.new_datastream(store, data_definition)
    assert datastream.cid is None
    assert list(datastream) == []

    records = make_records(n, data_definition)
    datastream = datastream.extend(records[: n // 2])
    assert datastream.cid is not None

    datastream = dc_banyan.load_datastream(datastream.cid, store, data_definition)
    stored = list(datastream)
    assert len(stored) == n // 2

    datastream = datastream.extend(records[n // 2 :])
    stored = list(datastream)
    assert len(stored) == n
    assert records == stored

    assert list(datastream[100:200]) == records[100:200]
    assert list(datastream[100:200][:10]) == records[100:110]
    assert list(datastream[100:200][10:]) == records[110:200]
    assert datastream[100] == records[100]
    with pytest.raises(NotImplementedError):
        datastream[100:200].extend(records)


def test_query(store, data_definition):
    n = 1000
    datastream = dc_banyan.new_datastream(store, data_definition)

    records = make_records(n, data_definition)
    datastream = datastream.extend(records)

    datastream = dc_banyan.load_datastream(datastream.cid, store, data_definition)
    ts = datetime.datetime.fromtimestamp(12)
    query = data_definition["ts"] == ts
    results = list(datastream.query(query))
    assert len(results) == 1
    assert results[0] == records[12]

    query = query | (data_definition["one"] <= 112)
    results = list(datastream.query(query))
    assert len(results) == 6
    assert results[0] == records[0]
    assert results[1] == records[1]
    assert results[2] == records[2]
    assert results[3] == records[3]
    assert results[4] == records[4]
    assert results[5] == records[12]

    results = list(datastream[4:].query(query))
    assert len(results) == 2
    assert results[0] == records[4]
    assert results[1] == records[12]


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


def test__repr__(store, data_definition):
    assert repr(store) in ("MemStore", "IpfsStore")
    assert '{ position: 0, name: "ts", kind: Timestamp, index: true }' in repr(
        data_definition
    )
    record = make_records(1, data_definition)[0]
    assert '"one": Integer(100)' in repr(record)


def test_as_dict(data_definition):
    record = make_records(1, data_definition)[0]
    assert record.as_dict() == {
        "eight": "boo",
        "five": "",
        "four": 0.0,
        "nine": datetime.datetime(1969, 12, 31, 18, 59, 54),
        "one": 100,
        "seven": "foo",
        "six": "",
        "three": 0.0,
        "ts": datetime.datetime(1969, 12, 31, 19, 0),
        "two": 0,
    }


def test_column_definition_getters(data_definition):
    assert data_definition["ts"].name == "ts"
    assert data_definition["ts"].type == dc_banyan.Timestamp
    assert data_definition["ts"].index

    assert data_definition["two"].type == dc_banyan.Integer
    assert not data_definition["two"].index

    assert data_definition["three"].type == dc_banyan.Float
    assert data_definition["five"].type == dc_banyan.String
    assert data_definition["seven"].type == dc_banyan.Enum("foo", "bar", "baz")


def test_data_definition_iter(data_definition):
    assert [col.name for col in data_definition] == [
        "ts",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
    ]
