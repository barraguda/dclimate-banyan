import csv
import dc_banyan


SIZE_64_MB = 1 << 26


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
    if column.kind == dc_banyan.Timestamp:
        raise NotImplementedError

    elif column.kind == dc_banyan.Float:
        return float(value)

    else:
        raise NotImplementedError(str(column.kind))


def read_csv(data_definition, stream):
    reader = csv.DictReader(stream)
    for row in reader:
        yield {key: parse(data_definition[key], value) for key, value in row}


def write_data(resolver, data_definition, records):
    datastream = resolver.new_datastream(data_definition)
    datastream = datastream.extend(records)

    return datastream.cid


def read_data(resolver, data_defintion, cid):
    datastream = resolver.load_datastream(cid, data_defintion)
    for record in datastream:
        print(f"read: {record}")


def usw_example(resolver):
    dd = usw_data_definition()
    records = read_csv(dd, open("../rust-example/USW00003927.csv"))
    cid = write_data(resolver, dd, records)
    read_data(resolver, dd, cid)

    return cid


def main():
    if dc_banyan.ipfs_available():
        resolver = dc_banyan.memory_resolver()
    else:
        resolver = dc_banyan.ipfs_resolver()

    cid = usw_example(resolver)
    print(f"cid: {cid}")


if __name__ == "__main__":
    main()
