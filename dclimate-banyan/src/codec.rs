use banyan::{index::UnitSeq, TreeTypes};
use banyan_utils::tags::Sha256Digest;
use cbor_data::codec::{CodecError, ReadCbor, WriteCbor};

use crate::{
    bitmap::Bitmap, data_definition::ColumnType, error::Result, value::Value, ConversionError,
};

#[derive(Clone, Debug)]
pub(crate) struct TreeType;

pub(crate) type TreeKey = ();
pub(crate) type TreeSummary = ();

impl TreeTypes for TreeType {
    type Key = TreeKey;
    type Summary = TreeSummary;
    type KeySeq = UnitSeq; //VecSeq<TreeKey>;
    type SummarySeq = UnitSeq; //VecSeq<TreeSummary>;
    type Link = Sha256Digest;
}

// struct TreeKey(BTreeMap<&'static str, TreeValue>);
// struct TreeSummary(BTreeMap<&'static str, SummaryValue>);

// enum SummaryValue {
//     Timestamp(SummaryRange<i64>),
//     Integer(SummaryRange<i64>),
//     Float(SummaryRange<f64>),
//     String(SummaryRange<String>),
//     //    Enum(SummaryBitmap),
// }

// struct SummaryRange<T: PartialOrd> {
//     lhs: T,
//     rhs: T,
// }

#[repr(u8)]
#[derive(Debug, PartialEq, ReadCbor, WriteCbor)]
pub(crate) enum TreeValue {
    Timestamp(i64),
    Integer(i64),
    Float(f64),
    String(String),
    Enum(u16),
}

impl TreeValue {
    pub(crate) fn from_value(kind: &ColumnType, value: Value) -> Result<Self> {
        match kind {
            ColumnType::Timestamp => Ok(TreeValue::Timestamp(value.try_into()?)),
            ColumnType::Integer => Ok(TreeValue::Integer(value.try_into()?)),
            ColumnType::Float => Ok(TreeValue::Float(value.try_into()?)),
            ColumnType::String => Ok(TreeValue::String(value.try_into()?)),
            ColumnType::Enum(choices) => {
                let error = ConversionError::new(&value, format!("{kind:?}"));
                let choice: String = value.try_into()?;
                for (i, option) in choices.iter().enumerate() {
                    if *option == &choice {
                        return Ok(TreeValue::Enum(i as u16));
                    }
                }
                Err(error)?
            }
        }
    }
}

impl TryFrom<TreeValue> for i64 {
    type Error = ConversionError;

    fn try_from(value: TreeValue) -> std::result::Result<Self, Self::Error> {
        match value {
            TreeValue::Timestamp(ts) => Ok(ts),
            TreeValue::Integer(n) => Ok(n),
            _ => Err(ConversionError::new(value, "i64")),
        }
    }
}

impl TryFrom<TreeValue> for f64 {
    type Error = ConversionError;

    fn try_from(value: TreeValue) -> std::result::Result<Self, Self::Error> {
        match value {
            TreeValue::Float(n) => Ok(n),
            _ => Err(ConversionError::new(value, "f64")),
        }
    }
}

impl TryFrom<TreeValue> for String {
    type Error = ConversionError;

    fn try_from(value: TreeValue) -> std::result::Result<Self, Self::Error> {
        match value {
            TreeValue::String(s) => Ok(s),
            _ => Err(ConversionError::new(value, "String")),
        }
    }
}

impl TryFrom<TreeValue> for usize {
    type Error = ConversionError;

    fn try_from(value: TreeValue) -> std::result::Result<Self, Self::Error> {
        match value {
            TreeValue::Enum(n) => Ok(n as usize),
            _ => Err(ConversionError::new(value, "usize")),
        }
    }
}

#[derive(ReadCbor, WriteCbor)]
pub(crate) struct Row(pub Bitmap, pub Vec<TreeValue>);

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;

    use crate::{ColumnType, Value};

    use super::*;

    #[test]
    fn test_tree_value_from_value() -> Result<()> {
        let value = Value::Timestamp(NaiveDateTime::from_timestamp_opt(1234, 5678).unwrap());
        let tree_value = TreeValue::from_value(&ColumnType::Timestamp, value)?;
        assert_eq!(tree_value, TreeValue::Timestamp(1234));

        let value = Value::Integer(5678);
        let tree_value = TreeValue::from_value(&ColumnType::Integer, value)?;
        assert_eq!(tree_value, TreeValue::Integer(5678));

        let value = Value::Float(56.78);
        let tree_value = TreeValue::from_value(&ColumnType::Float, value)?;
        assert_eq!(tree_value, TreeValue::Float(56.78));

        let value = Value::String("Hi Mom!".into());
        let tree_value = TreeValue::from_value(&ColumnType::String, value)?;
        assert_eq!(tree_value, TreeValue::String(String::from("Hi Mom!")));

        let column = ColumnType::Enum(vec!["zero", "one", "two"]);
        let value = Value::String("one".into());
        let tree_value = TreeValue::from_value(&column, value)?;
        assert_eq!(tree_value, TreeValue::Enum(1));

        Ok(())
    }
}
