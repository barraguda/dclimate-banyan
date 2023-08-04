use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dclimate_banyan::{
    ipfs_available, memory_store, BanyanStore,
    ColumnType::{Float, Timestamp},
    DataDefinition, Datastream, IpfsStore, Record, Result, Value,
};
use libipld::Cid;

const SIZE_64_MB: usize = 1 << 26;

fn usw_data_definition() -> DataDefinition {
    DataDefinition::from_iter(vec![
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
    ])
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
        for column in dd.columns() {
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

fn usw_example<S: BanyanStore>(store: &S) -> Result<Cid> {
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

    let datastream = Datastream::new(store.clone(), dd.clone());
    let datastream = datastream.extend(records)?;
    let cid = datastream.cid.unwrap();

    let datastream = Datastream::load(&cid, store.clone(), dd.clone());
    for record in datastream.iter()? {
        println!("read: {:?}", *record?);
    }

    let ts = dd.get_by_name("DATE").unwrap();
    let start = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1975, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let end = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1976, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let ts_query = ts
        .ge(Value::Timestamp(start))
        .and(ts.lt(Value::Timestamp(end)))?;

    let prcp = dd.get_by_name("PRCP").unwrap();
    let prcp_query = prcp.gt(Value::Float(0.0))?;
    let query = ts_query.and(prcp_query);

    let datastream = Datastream::load(&cid, store.clone(), dd.clone());
    let results = datastream.query(&query)?;
    for record in results {
        println!("result: {:?}", *record?);
    }

    Ok(cid)
}

fn main() -> Result<()> {
    if ipfs_available() {
        println!("IPFS available");
        let cid = usw_example(&IpfsStore)?;

        println!("{cid:?}");
    } else {
        println!("IPFS not available, using memory store");
        usw_example(&memory_store(SIZE_64_MB))?;
    };

    Ok(())
}
