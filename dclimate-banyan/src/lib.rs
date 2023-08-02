mod bitmap;
mod codec;
mod data_definition;
mod datastream;
mod error;
mod query;
mod resolver;
mod value;

use banyan::store::{BlockWriter, ReadOnlyStore};
use banyan_utils::tags::Sha256Digest;

pub use banyan_utils::ipfs::IpfsStore;

pub use data_definition::{ColumnDefinition, ColumnType, DataDefinition, Record};
pub use datastream::Datastream;
pub use error::{ConversionError, Result};
pub use query::Query;
pub use resolver::{ipfs_available, memory_store, Resolver};
pub use value::Value;

pub trait BanyanStore: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Sized {}

impl<S> BanyanStore for S where S: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> {}

#[cfg(test)]
mod integration_tests {
    use super::*;

    use chrono::NaiveDateTime;

    const SIZE_64_MB: usize = 1 << 26;

    fn get_data_definition() -> DataDefinition {
        DataDefinition::from_iter(vec![
            ("ts", ColumnType::Timestamp, true),
            ("one", ColumnType::Integer, true),
            ("two", ColumnType::Integer, false),
            ("three", ColumnType::Float, true),
            ("four", ColumnType::Float, false),
            ("five", ColumnType::String, true),
            ("six", ColumnType::String, false),
            // ("seven", ColumnType::Enum(vec!["foo", "bar", "baz"]), true),
            // ("eight", ColumnType::Enum(vec!["boo", "far", "faz"]), true),
            (
                "seven",
                ColumnType::Enum(vec!["foo".into(), "bar".into(), "baz".into()]),
                false,
            ),
            (
                "eight",
                ColumnType::Enum(vec!["boo".into(), "far".into(), "faz".into()]),
                false,
            ),
            ("nine", ColumnType::Timestamp, false),
        ])
    }

    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    fn test_string(i: i32) -> String {
        let mut i = i as usize;
        let mut s = String::new();
        assert_eq!(ALPHA.len(), 52);
        loop {
            let char = i % 52;
            s.push(ALPHA[char] as char);

            i /= 52;

            if i == 0 {
                break;
            }
        }

        s
    }

    fn make_records<'ds>(n: i32, definition: &'ds DataDefinition) -> Vec<Record<'ds>> {
        let seven_values = vec!["foo", "bar", "baz"];
        let eight_values = vec!["boo", "far", "faz"];

        let mut records = vec![];
        for i in 0..n {
            let mut record = definition.record();
            record.insert(
                "ts".into(),
                Value::from(NaiveDateTime::from_timestamp_opt(i as i64, 0).unwrap()),
            );
            record.insert("one".into(), Value::from(100 + i * 3));
            if i % 2 == 0 {
                record.insert("two".into(), Value::from(i + i * 2));
            }
            if i % 3 == 1 {
                record.insert("three".into(), Value::from((i as f64) / 1.2));
            }
            if i % 4 != 2 {
                record.insert("four".into(), Value::from((i as f64) * 3.141592));
            }
            if i % 5 == 0 {
                record.insert("five".into(), Value::from(test_string(i)));
            }
            if i % 2 == 0 {
                record.insert("six".into(), Value::from(test_string(i * 1013)));
            }
            record.insert("seven".into(), Value::from(seven_values[i as usize % 3]));
            if i % 3 == 0 {
                record.insert("eight".into(), Value::from(eight_values[i as usize % 3]));
            }
            if i % 5 != 1 {
                record.insert(
                    "nine".into(),
                    Value::from(NaiveDateTime::from_timestamp_opt(i as i64 * 512 - 6, 0).unwrap()),
                );
            }

            records.push(record);
        }

        records
    }

    #[test]
    fn test_codec() -> Result<()> {
        let definition = get_data_definition();
        let store = memory_store(SIZE_64_MB);
        let resolver = Resolver::new(store);
        let datastream = resolver.new_datastream(&definition);

        let n = 100000;
        let all_records = make_records(n, &definition);
        let mut records_iter = all_records.clone().into_iter();
        let records: Vec<Record> = records_iter.by_ref().take(n as usize / 2).collect();
        let datastream = datastream.extend(records)?;

        let datastream = resolver.load_datastream(&datastream.cid.unwrap(), &definition);
        let stored: Vec<Record> = datastream.iter()?.collect::<Result<Vec<Record>>>()?;
        assert_eq!(stored.len(), n as usize / 2);

        let records: Vec<Record> = records_iter.collect();
        assert_eq!(records.len(), n as usize / 2);
        let datastream = datastream.extend(records)?;
        let stored = datastream.iter()?.collect::<Result<Vec<Record>>>()?;
        assert_eq!(stored.len(), n as usize);

        assert_eq!(all_records, stored);

        Ok(())
    }

    #[test]
    fn test_query() -> Result<()> {
        let definition = get_data_definition();
        let store = memory_store(SIZE_64_MB);
        let resolver = Resolver::new(store);
        let datastream = resolver.new_datastream(&definition);

        let n = 100000;
        let records = make_records(n, &definition);
        let datastream = datastream.extend(records.clone())?;

        let datastream = resolver.load_datastream(&datastream.cid.unwrap(), &definition);
        let ts = NaiveDateTime::from_timestamp_opt(12, 0).unwrap();
        let ts = Value::Timestamp(ts);
        let query = definition.get_by_name("ts").unwrap().eq(ts)?;
        let results: Vec<Record> = datastream.query(&query)?.collect::<Result<Vec<Record>>>()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], records[12]);

        let query = query.or(definition
            .get_by_name("one")
            .unwrap()
            .le(Value::Integer(112))?);
        let results: Vec<Record> = datastream.query(&query)?.collect::<Result<Vec<Record>>>()?;
        assert_eq!(results.len(), 6);
        assert_eq!(results[0], records[0]);
        assert_eq!(results[1], records[1]);
        assert_eq!(results[2], records[2]);
        assert_eq!(results[3], records[3]);
        assert_eq!(results[4], records[4]);
        assert_eq!(results[5], records[12]);

        Ok(())
    }
}
