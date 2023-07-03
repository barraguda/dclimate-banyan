use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dclimate_banyan::{
    memory_store, BanyanStore, ColumnDefinition,
    ColumnType::{Float, Timestamp},
    DataDefinition, Record, Resolver, Result, Value,
};
use libipld::Cid;

const SIZE_64_MB: usize = 1 << 26;

fn usw_data_definition() -> DataDefinition {
    DataDefinition(
        [
            ("DATE", Timestamp, true),
            ("ACMH", Float, false),
            ("ACSH", Float, false),
            ("AWND", Float, false),
            ("FMTM", Float, false),
            ("GAHT", Float, false),
            ("PGTM", Float, false),
            ("PRCP", Float, true),
            ("PSUN", Float, false),
            ("SNOW", Float, false),
            ("SNWD", Float, false),
            ("TAVG", Float, false),
            ("TMAX", Float, true),
            ("TMIN", Float, false),
            ("TSUN", Float, false),
            ("WDF1", Float, false),
            ("WDF2", Float, false),
            ("WDF5", Float, false),
            ("WDFG", Float, false),
            ("WESD", Float, false),
            ("WSF1", Float, false),
            ("WSF2", Float, false),
            ("WSF5", Float, false),
            ("WSFG", Float, false),
            ("WT01", Float, false),
            ("WT02", Float, false),
            ("WT03", Float, false),
            ("WT04", Float, false),
            ("WT05", Float, false),
            ("WT06", Float, false),
            ("WT07", Float, false),
            ("WT08", Float, false),
            ("WT09", Float, false),
            ("WT10", Float, false),
            ("WT11", Float, false),
            ("WT13", Float, false),
            ("WT14", Float, false),
            ("WT15", Float, false),
            ("WT16", Float, false),
            ("WT17", Float, false),
            ("WT18", Float, false),
            ("WT19", Float, false),
            ("WT21", Float, false),
            ("WT22", Float, false),
            ("WV03", Float, false),
        ]
        .into_iter()
        .map(|(name, kind, index)| ColumnDefinition::new(name, kind, index))
        .collect(),
    )
}

fn record_from_csv<'dd>(
    dd: &'dd DataDefinition,
    csv: BTreeMap<&String, String>,
) -> Result<Record<'dd>> {
    let mut record = dd.record();
    for (name, value) in csv {
        if value.len() == 0 {
            continue;
        }
        for column in &dd.0 {
            if *column.name == *name {
                match column.kind {
                    Timestamp => {
                        let date = value.parse::<NaiveDate>()?;
                        let dt =
                            NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                        record.insert(name.clone(), Value::from(dt));
                    }
                    Float => {
                        record.insert(name.clone(), Value::from(value.parse::<f64>()?));
                    }
                    _ => panic!("Not expected {:?}", column.kind),
                }
            }
        }
    }
    Ok(record)
}

fn write_data<'dd, I, S: BanyanStore>(
    resolver: &'dd Resolver<S>,
    dd: &'dd DataDefinition,
    records: I,
) -> Result<Cid>
where
    I: IntoIterator<Item = Record<'dd>>,
{
    let datastream = resolver.new_datastream(dd);
    let datastream = datastream.extend(records)?;

    Ok(datastream.cid.unwrap())
}

fn read_data<S: BanyanStore>(resolver: &Resolver<S>, dd: &DataDefinition, cid: &Cid) -> Result<()> {
    let datastream = resolver.load_datastream(cid, dd);
    for record in datastream.iter()? {
        println!("{:?}", *record?);
    }

    Ok(())
}

fn usw_example<S: BanyanStore>(resolver: &Resolver<S>) -> Result<()> {
    let dd = usw_data_definition();
    let mut reader = csv::Reader::from_path("rust-example/USW00003927.csv")?;
    let headers = reader
        .headers()?
        .iter()
        .map(String::from)
        .collect::<Vec<_>>();

    let mut records = vec![];
    for result in reader.records() {
        let row = result?;
        let record = BTreeMap::from_iter(headers.iter().zip(row.into_iter().map(String::from)));
        records.push(record_from_csv(&dd, record)?);
    }

    let cid = write_data(resolver, &dd, records)?;
    read_data(resolver, &dd, &cid)?;

    Ok(())
}

fn main() -> Result<()> {
    let store = memory_store(SIZE_64_MB);
    let resolver = Resolver::new(store);
    usw_example(&resolver)?;

    Ok(())
}
