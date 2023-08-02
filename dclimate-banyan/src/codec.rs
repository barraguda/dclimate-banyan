use std::io::{Read, Seek, Write};

use banyan::{
    index::{Summarizable, VecSeq},
    TreeTypes,
};
use banyan_utils::tags::Sha256Digest;
use cbor_data::codec::{CodecError, ReadCbor, WriteCbor};
use libipld::{
    cbor::DagCborCodec,
    prelude::{Decode, Encode},
};

use crate::{
    bitmap::Bitmap,
    data_definition::ColumnType,
    error::{DecodeError, Result},
    value::Value,
    ConversionError,
};

#[derive(Clone, Debug)]
pub(crate) struct TreeType;

impl TreeTypes for TreeType {
    type Key = TreeKey;
    type Summary = TreeSummary;
    type KeySeq = VecSeq<TreeKey>;
    type SummarySeq = VecSeq<TreeSummary>;
    type Link = Sha256Digest;
}

pub(crate) type TreeKey = Row;

#[derive(Clone, Debug, PartialEq)]
pub struct TreeSummary(pub(crate) Bitmap, pub(crate) Vec<SummaryValue>);
impl TreeSummary {
    pub(crate) fn get(&self, position: usize) -> Option<&SummaryValue> {
        if self.0.get(position) {
            Some(&self.1[(self.0.rank(position + 1) - 1) as usize])
        } else {
            None
        }
    }
}

impl Summarizable<TreeSummary> for VecSeq<TreeSummary> {
    fn summarize(&self) -> TreeSummary {
        let mut columns = Bitmap::new();
        let mut summaries: Vec<Option<SummaryValue>> = vec![None; 64];
        for child in self.as_ref().iter().cloned() {
            columns |= child.0;
            let mut values = child.1;
            let mut index = 0;
            while values.len() > 0 {
                if child.0.get(index) {
                    let value = values.remove(0);
                    summaries[index] = Some(match &summaries[index] {
                        None => value,
                        Some(summary) => summary.extend(value).unwrap(),
                    });
                }
                index += 1;
            }
        }

        let summaries = summaries
            .into_iter()
            .filter(|v| v.is_some())
            .map(|v| v.unwrap())
            .collect();

        TreeSummary(columns, summaries)
    }
}

impl Encode<DagCborCodec> for TreeSummary {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        self.0.encode(c, w)?;
        self.1.encode(c, w)?;

        Ok(())
    }
}

impl Decode<DagCborCodec> for TreeSummary {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
        let columns = Bitmap::decode(c, r)?;
        let summaries = Vec::decode(c, r)?;

        Ok(Self(columns, summaries))
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SummaryValue {
    Timestamp(SummaryRange<i64>),
    Integer(SummaryRange<i64>),
    Float(SummaryRange<f64>),
    String(SummaryRange<String>),
    //    Enum(SummaryBitmap),
}

impl SummaryValue {
    fn incorporate(&self, value: TreeValue) -> Self {
        match self {
            Self::Timestamp(range) => {
                Self::Timestamp(range.incorporate(i64::try_from(value).unwrap()))
            }
            Self::Integer(range) => Self::Integer(range.incorporate(i64::try_from(value).unwrap())),
            Self::Float(range) => Self::Float(range.incorporate(f64::try_from(value).unwrap())),
            Self::String(range) => {
                Self::String(range.incorporate(String::try_from(value).unwrap()))
            }
        }
    }

    fn extend(&self, value: SummaryValue) -> Result<Self> {
        let summary = match self {
            Self::Timestamp(range) => Self::Timestamp(range.extend(value.try_into()?)),
            Self::Integer(range) => Self::Integer(range.extend(value.try_into()?)),
            Self::Float(range) => Self::Float(range.extend(value.try_into()?)),
            Self::String(range) => Self::String(range.extend(value.try_into()?)),
        };

        Ok(summary)
    }

    fn discriminant(&self) -> u8 {
        unsafe { *(self as *const Self as *const u8) }
    }
}

impl From<TreeValue> for SummaryValue {
    fn from(value: TreeValue) -> Self {
        match value {
            TreeValue::Timestamp(n) => Self::Timestamp(SummaryRange { lhs: n, rhs: n }),
            TreeValue::Integer(n) => Self::Integer(SummaryRange { lhs: n, rhs: n }),
            TreeValue::Float(n) => Self::Float(SummaryRange { lhs: n, rhs: n }),
            TreeValue::String(s) => Self::String(SummaryRange {
                lhs: s.clone(),
                rhs: s,
            }),
            TreeValue::Enum(_) => todo!(),
        }
    }
}

impl Encode<DagCborCodec> for SummaryValue {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        self.discriminant().encode(c, w)?;
        match self {
            Self::Timestamp(range) => {
                range.encode(c, w)?;
            }
            Self::Integer(range) => {
                range.encode(c, w)?;
            }
            Self::Float(range) => {
                range.encode(c, w)?;
            }
            Self::String(range) => {
                range.encode(c, w)?;
            }
        }

        Ok(())
    }
}

impl Decode<DagCborCodec> for SummaryValue {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
        let kind = u8::decode(c, r)?;
        let summary = match kind {
            0 => Self::Timestamp(SummaryRange::decode(c, r)?),
            1 => Self::Integer(SummaryRange::decode(c, r)?),
            2 => Self::Float(SummaryRange::decode(c, r)?),
            3 => Self::String(SummaryRange::decode(c, r)?),
            _ => Err(DecodeError::new(kind, "SummaryValue type"))?,
        };

        Ok(summary)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SummaryRange<T>
where
    T: PartialOrd + Clone,
{
    pub(crate) lhs: T,
    pub(crate) rhs: T,
}

impl<T> SummaryRange<T>
where
    T: PartialOrd + Clone,
{
    fn incorporate(&self, value: T) -> Self {
        let lhs = if value < self.lhs {
            value.clone()
        } else {
            self.lhs.clone()
        };
        let rhs = if value > self.rhs {
            value
        } else {
            self.rhs.clone()
        };

        Self { lhs, rhs }
    }

    fn extend(&self, range: SummaryRange<T>) -> SummaryRange<T> {
        let lhs = if range.lhs < self.lhs {
            range.lhs.clone()
        } else {
            self.lhs.clone()
        };
        let rhs = if range.rhs > self.rhs {
            range.rhs.clone()
        } else {
            self.rhs.clone()
        };

        Self { lhs, rhs }
    }
}

impl TryFrom<SummaryValue> for SummaryRange<i64> {
    type Error = ConversionError;

    fn try_from(value: SummaryValue) -> std::result::Result<Self, Self::Error> {
        match value {
            SummaryValue::Timestamp(range) => Ok(range),
            SummaryValue::Integer(range) => Ok(range),
            _ => Err(ConversionError::new(value, "SummaryRange<i64>")),
        }
    }
}

impl TryFrom<SummaryValue> for SummaryRange<f64> {
    type Error = ConversionError;

    fn try_from(value: SummaryValue) -> std::result::Result<Self, Self::Error> {
        match value {
            SummaryValue::Float(range) => Ok(range),
            _ => Err(ConversionError::new(value, "SummaryRange<f64>")),
        }
    }
}

impl TryFrom<SummaryValue> for SummaryRange<String> {
    type Error = ConversionError;

    fn try_from(value: SummaryValue) -> std::result::Result<Self, Self::Error> {
        match value {
            SummaryValue::String(range) => Ok(range),
            _ => Err(ConversionError::new(value, "SummaryRange<String>")),
        }
    }
}

impl<T> Encode<DagCborCodec> for SummaryRange<T>
where
    T: Encode<DagCborCodec> + PartialOrd + Clone,
{
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        self.lhs.encode(c, w)?;
        self.rhs.encode(c, w)?;

        Ok(())
    }
}

impl<T> Decode<DagCborCodec> for SummaryRange<T>
where
    T: Decode<DagCborCodec> + PartialOrd + Clone,
{
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        let lhs = T::decode(c, r)?;
        let rhs = T::decode(c, r)?;

        Ok(Self { lhs, rhs })
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, ReadCbor, WriteCbor)]
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
                    if option == &choice {
                        return Ok(TreeValue::Enum(i as u16));
                    }
                }
                Err(error)?
            }
        }
    }

    fn discriminant(&self) -> u8 {
        unsafe { *(self as *const Self as *const u8) }
    }
}

impl Encode<DagCborCodec> for TreeValue {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        self.discriminant().encode(c, w)?;
        match self {
            Self::Timestamp(n) => {
                n.encode(c, w)?;
            }
            Self::Integer(n) => {
                n.encode(c, w)?;
            }
            Self::Float(n) => {
                n.encode(c, w)?;
            }
            Self::String(s) => {
                s.encode(c, w)?;
            }
            Self::Enum(n) => {
                n.encode(c, w)?;
            }
        }

        Ok(())
    }
}

impl Decode<DagCborCodec> for TreeValue {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
        let kind = u8::decode(c, r)?;
        let decoded = match kind {
            0 => Self::Timestamp(i64::decode(c, r)?),
            1 => Self::Integer(i64::decode(c, r)?),
            2 => Self::Float(f64::decode(c, r)?),
            3 => Self::String(String::decode(c, r)?),
            4 => Self::Enum(u16::decode(c, r)?),
            _ => Err(DecodeError::new(kind, "TreeValue type"))?,
        };

        Ok(decoded)
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

#[derive(Clone, Debug, PartialEq, ReadCbor, WriteCbor)]
pub(crate) struct Row(pub Bitmap, pub Vec<TreeValue>);

impl Row {
    pub(crate) fn get(&self, position: usize) -> Option<&TreeValue> {
        if self.0.get(position) {
            Some(&self.1[(self.0.rank(position + 1) - 1) as usize])
        } else {
            None
        }
    }
}

impl Summarizable<TreeSummary> for VecSeq<Row> {
    fn summarize(&self) -> TreeSummary {
        let mut columns = Bitmap::new();
        let mut summaries: Vec<Option<SummaryValue>> = vec![None; 64];
        for row in self.as_ref().iter().cloned() {
            columns |= row.0;
            let mut values = row.1;
            let mut index = 0;
            while values.len() > 0 {
                if row.0.get(index) {
                    let value = values.remove(0);
                    summaries[index] = Some(match &summaries[index] {
                        None => SummaryValue::from(value),
                        Some(summary) => summary.incorporate(value),
                    });
                }
                index += 1;
            }
        }

        let summaries = summaries
            .into_iter()
            .filter(|v| v.is_some())
            .map(|v| v.unwrap())
            .collect();

        TreeSummary(columns, summaries)
    }
}

impl Encode<DagCborCodec> for Row {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        self.0.encode(c, w)?;
        self.1.encode(c, w)
    }
}

impl Decode<DagCborCodec> for Row {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
        let columns = Bitmap::decode(c, r)?;
        let values = Vec::decode(c, r)?;

        Ok(Row(columns, values))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

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

        let column = ColumnType::Enum(vec!["zero".into(), "one".into(), "two".into()]);
        let value = Value::String("one".into());
        let tree_value = TreeValue::from_value(&column, value)?;
        assert_eq!(tree_value, TreeValue::Enum(1));

        Ok(())
    }

    fn test_encode_decode<T: Encode<DagCborCodec> + Decode<DagCborCodec>>(value: T) -> Result<T> {
        let mut buf: Vec<u8> = Vec::new();
        value.encode(DagCborCodec, &mut buf)?;

        let mut cursor = Cursor::new(buf);
        T::decode(DagCborCodec, &mut cursor)
    }

    #[test]
    fn test_tree_value_codec() -> Result<()> {
        test_encode_decode(TreeValue::Timestamp(1234))?;
        test_encode_decode(TreeValue::Integer(5678))?;
        test_encode_decode(TreeValue::Float(56.78))?;
        test_encode_decode(TreeValue::String("Hi Mom!".into()))?;
        test_encode_decode(TreeValue::Enum(42))?;
        Ok(())
    }

    #[test]
    fn test_summary_value_from_tree_value() -> Result<()> {
        let value = TreeValue::Timestamp(1234);
        let summary: SummaryValue = value.into();
        let expected = SummaryValue::Timestamp(SummaryRange {
            lhs: 1234,
            rhs: 1234,
        });
        assert_eq!(summary, expected);

        let value = TreeValue::Integer(5678);
        let summary: SummaryValue = value.into();
        let expected = SummaryValue::Integer(SummaryRange {
            lhs: 5678,
            rhs: 5678,
        });
        assert_eq!(summary, expected);

        let value = TreeValue::Float(56.78);
        let summary: SummaryValue = value.into();
        let expected = SummaryValue::Float(SummaryRange {
            lhs: 56.78,
            rhs: 56.78,
        });
        assert_eq!(summary, expected);

        let value = TreeValue::String("Aardvark".into());
        let summary: SummaryValue = value.into();
        let expected = SummaryValue::String(SummaryRange {
            lhs: "Aardvark".into(),
            rhs: "Aardvark".into(),
        });
        assert_eq!(summary, expected);

        Ok(())
    }

    #[test]
    fn test_summary_value_incorporate() -> Result<()> {
        let value = TreeValue::Timestamp(5678);
        let summary = SummaryValue::Timestamp(SummaryRange {
            lhs: 1234,
            rhs: 1234,
        });
        let expected = SummaryValue::Timestamp(SummaryRange {
            lhs: 1234,
            rhs: 5678,
        });
        assert_eq!(summary.incorporate(value), expected);

        let value = TreeValue::Integer(1234);
        let summary = SummaryValue::Integer(SummaryRange {
            lhs: 5678,
            rhs: 5678,
        });
        let expected = SummaryValue::Integer(SummaryRange {
            lhs: 1234,
            rhs: 5678,
        });
        assert_eq!(summary.incorporate(value), expected);

        let value = TreeValue::Float(12.34);
        let summary = SummaryValue::Float(SummaryRange {
            lhs: 56.78,
            rhs: 56.78,
        });
        let expected = SummaryValue::Float(SummaryRange {
            lhs: 12.34,
            rhs: 56.78,
        });
        assert_eq!(summary.incorporate(value), expected);

        let value = TreeValue::String("Aardvark".into());
        let summary = SummaryValue::String(SummaryRange {
            lhs: "Moose".into(),
            rhs: "Zebra".into(),
        });
        let expected = SummaryValue::String(SummaryRange {
            lhs: "Aardvark".into(),
            rhs: "Zebra".into(),
        });
        assert_eq!(summary.incorporate(value), expected);

        Ok(())
    }
}
