mod bitmap;
mod codec;
mod data_definition;
mod datastream;
mod error;
mod resolver;
mod value;

use banyan::store::{BlockWriter, ReadOnlyStore};
use banyan_utils::tags::Sha256Digest;

pub use data_definition::{ColumnDefinition, ColumnType, DataDefinition, Record};
pub use error::{ConversionError, Result};
pub use resolver::{memory_store, Resolver};
pub use value::Value;

pub trait BanyanStore: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Sized {}

impl<S> BanyanStore for S where S: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> {}

#[cfg(test)]
mod integration_tests {
    use super::*;

    use chrono::NaiveDateTime;

    const SIZE_64_MB: usize = 1 << 26;

    fn get_data_definition() -> DataDefinition {
        DataDefinition(vec![
            ColumnDefinition::new("ts", ColumnType::Timestamp, true),
            ColumnDefinition::new("one", ColumnType::Integer, true),
            ColumnDefinition::new("two", ColumnType::Integer, false),
            ColumnDefinition::new("three", ColumnType::Float, true),
            ColumnDefinition::new("four", ColumnType::Float, false),
            ColumnDefinition::new("five", ColumnType::String, true),
            ColumnDefinition::new("six", ColumnType::String, false),
            ColumnDefinition::new("seven", ColumnType::Enum(vec!["foo", "bar", "baz"]), true),
            ColumnDefinition::new("eight", ColumnType::Enum(vec!["boo", "far", "faz"]), true),
            ColumnDefinition::new("nine", ColumnType::Timestamp, false),
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

    fn make_records<'ds>(definition: &'ds DataDefinition) -> Vec<Record<'ds>> {
        let seven_values = vec!["foo", "bar", "baz"];
        let eight_values = vec!["boo", "far", "faz"];

        let mut records = vec![];
        for i in 0..1000 {
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

        let records = make_records(&definition);
        let datastream = datastream.extend(records.clone())?;

        let datastream = resolver.load_datastream(&datastream.cid.unwrap(), &definition);
        let stored: Vec<Record> = datastream.iter()?.collect::<Result<Vec<Record>>>()?;

        assert_eq!(records, stored);

        Ok(())
    }
}
