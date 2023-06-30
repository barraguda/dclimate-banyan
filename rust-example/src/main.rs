use std::collections::BTreeMap;

use chrono::NaiveDateTime;
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
            ("date", Timestamp, true),
            ("acmh", Float, false),
            ("acsh", Float, false),
            ("awnd", Float, false),
            ("fmtm", Float, false),
            ("gaht", Float, false),
            ("pgtm", Float, false),
            ("prcp", Float, false),
            ("psun", Float, false),
            ("snow", Float, false),
            ("snwd", Float, false),
            ("tavg", Float, false),
            ("tmax", Float, false),
            ("tmin", Float, false),
            ("tsun", Float, false),
            ("wdf1", Float, false),
            ("wdf2", Float, false),
            ("wdf5", Float, false),
            ("wdfg", Float, false),
            ("wesd", Float, false),
            ("wsf1", Float, false),
            ("wsf2", Float, false),
            ("wsf5", Float, false),
            ("wsfg", Float, false),
            ("wt01", Float, false),
            ("wt02", Float, false),
            ("wt03", Float, false),
            ("wt04", Float, false),
            ("wt05", Float, false),
            ("wt06", Float, false),
            ("wt07", Float, false),
            ("wt08", Float, false),
            ("wt09", Float, false),
            ("wt10", Float, false),
            ("wt11", Float, false),
            ("wt13", Float, false),
            ("wt14", Float, false),
            ("wt15", Float, false),
            ("wt16", Float, false),
            ("wt17", Float, false),
            ("wt18", Float, false),
            ("wt19", Float, false),
            ("wt21", Float, false),
            ("wt22", Float, false),
            ("wv03", Float, false),
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
                        record.insert(name.clone(), Value::from(value.parse::<NaiveDateTime>()?));
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
        println!("{record:?}");
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
