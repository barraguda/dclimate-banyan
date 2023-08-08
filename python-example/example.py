"""
Example of using `dc_banyan` to store station data.
"""
import csv
import datetime
import os
import pprint

import dc_banyan


SIZE_64_MB = 1 << 26


def main():
    """
    First we need to define the data we're going to be storing. This cannot change once
    you've started writing data for a datastream. A new data definition requires a new
    stream. This includes any columns declared as indexes. Indexes cannot be generated
    post facto.

    The definition itself is somewhat verbose so we just call a function here. See the
    function below for the full data definition.
    """
    data_definition = usw_data_definition()

    """
    For this example, we read in some records from a CSV file. This part doesn't have
    anything to with dc_banyan, so it's encapsulated in a function below. This function
    returns a generator that yields dictionaries containing data corresponding to the
    data definition.
    """
    records = read_usw_data(data_definition)

    """
    We have to use the Banyan data definition to create a record that can be stored in
    Banyan. (I will refactor to get rid of this step, soon, as this really should be
    unnecessary.)
    """
    records = [data_definition.record(record) for record in records]

    """
    Next we need a store. There are only two choices here. There is a memory based store
    that is useful for testing, and an IPFS store that talks to a locally running IPFS
    daemon via the HTTP API.
    """
    store = (
        dc_banyan.ipfs_store()
        if dc_banyan.ipfs_available()
        else dc_banyan.memory_store(SIZE_64_MB)
    )

    """
    We're starting a new datastream, so we'll create one that uses our data dictionary.
    """
    datastream = dc_banyan.new_datastream(store, data_definition)

    """
    Now we just need to add our records to the datastream. Since Banyan makes use of
    immutable data structures, this yields a new datastream with the new data added to
    it.
    """
    datastream = datastream.extend(records)

    """
    Our data is written and we now have a CID we can use to get back to it.
    """
    cid = datastream.cid
    print(f"CID: {cid}")

    """
    That's it really. Data can only be appended. The data dictionary can never change.
    Using the CID, we can load the data_definition and get data back out of it.
    """
    datastream = dc_banyan.load_datastream(cid, store, data_definition)

    """
    Use slices to retrieve data by index from the stream. Let's look at rows 10-19.
    """
    rows = [row.as_dict() for row in datastream[10:20]]
    pprint.pprint(rows)

    """
    Use indexing to retrieve records using any of the indexed columns. We can use the
    DATE column to get records from the first week of July 1975.
    """
    july1 = datetime.datetime(1975, 7, 1, 0, 0, 0)
    july8 = july1 + datetime.timedelta(days=7)
    datecol = data_definition["DATE"]
    query = (datecol >= july1) & (datecol < july8)
    rows = [row.as_dict() for row in datastream.query(query)]
    pprint.pprint(rows)

    """
    Or, let's find out how many days with precipitation there were in 1984.
    """
    precip = data_definition["PRCP"]
    start = datetime.datetime(1984, 1, 1, 0, 0, 0)
    end = datetime.datetime(1985, 1, 1, 0, 0, 0)
    query = (datecol >= start) & (datecol < end) & (precip > 0.0)
    rows = list(datastream.query(query))
    print(f"In 1984 there were {len(rows)} days with precipitation.")


def usw_data_definition():
    return dc_banyan.DataDefinition(
        (
            ("DATE", dc_banyan.Timestamp, True),
            ("ACMH", dc_banyan.Float, False),
            ("ACSH", dc_banyan.Float, False),
            ("AWND", dc_banyan.Float, False),
            ("FMTM", dc_banyan.Float, False),
            ("GAHT", dc_banyan.Float, False),
            ("PGTM", dc_banyan.Float, False),
            ("PRCP", dc_banyan.Float, True),
            ("PSUN", dc_banyan.Float, False),
            ("SNOW", dc_banyan.Float, False),
            ("SNWD", dc_banyan.Float, False),
            ("TAVG", dc_banyan.Float, False),
            ("TMAX", dc_banyan.Float, True),
            ("TMIN", dc_banyan.Float, False),
            ("TSUN", dc_banyan.Float, False),
            ("WDF1", dc_banyan.Float, False),
            ("WDF2", dc_banyan.Float, False),
            ("WDF5", dc_banyan.Float, False),
            ("WDFG", dc_banyan.Float, False),
            ("WESD", dc_banyan.Float, False),
            ("WSF1", dc_banyan.Float, False),
            ("WSF2", dc_banyan.Float, False),
            ("WSF5", dc_banyan.Float, False),
            ("WSFG", dc_banyan.Float, False),
            ("WT01", dc_banyan.Float, False),
            ("WT02", dc_banyan.Float, False),
            ("WT03", dc_banyan.Float, False),
            ("WT04", dc_banyan.Float, False),
            ("WT05", dc_banyan.Float, False),
            ("WT06", dc_banyan.Float, False),
            ("WT07", dc_banyan.Float, False),
            ("WT08", dc_banyan.Float, False),
            ("WT09", dc_banyan.Float, False),
            ("WT10", dc_banyan.Float, False),
            ("WT11", dc_banyan.Float, False),
            ("WT13", dc_banyan.Float, False),
            ("WT14", dc_banyan.Float, False),
            ("WT15", dc_banyan.Float, False),
            ("WT16", dc_banyan.Float, False),
            ("WT17", dc_banyan.Float, False),
            ("WT18", dc_banyan.Float, False),
            ("WT19", dc_banyan.Float, False),
            ("WT21", dc_banyan.Float, False),
            ("WT22", dc_banyan.Float, False),
            ("WV03", dc_banyan.Float, False),
        )
    )


def parse(column, value):
    if column.type == dc_banyan.Timestamp:
        year, month, day = map(int, value.split("-"))
        return datetime.datetime(year, month, day, 0, 0, 0)

    elif column.type == dc_banyan.Float:
        return float(value)

    else:
        raise NotImplementedError(str(column.type))


def read_usw_data(data_definition):
    exclude_keys = {"STATION", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"}
    here = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.abspath(
        os.path.join(here, "..", "rust-example", "USW00003927.csv")
    )
    reader = csv.DictReader(open(csv_path))
    for row in reader:
        yield {
            key: parse(data_definition[key], value)
            for key, value in row.items()
            if key not in exclude_keys and value != ""
        }


if __name__ == "__main__":
    main()
