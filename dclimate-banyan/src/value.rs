use chrono::NaiveDateTime;

use crate::{
    codec::TreeValue,
    data_definition::ColumnType,
    error::{ConversionError, Result},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Timestamp(NaiveDateTime),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub(crate) fn from_tree_value(kind: &ColumnType, tree_value: TreeValue) -> Result<Self> {
        match kind {
            ColumnType::Timestamp => Ok(Value::Timestamp(
                NaiveDateTime::from_timestamp_opt(tree_value.try_into()?, 0).unwrap(),
            )),
            ColumnType::Integer => Ok(Value::Integer(tree_value.try_into()?)),
            ColumnType::Float => Ok(Value::Float(tree_value.try_into()?)),
            ColumnType::String => Ok(Value::String(tree_value.try_into()?)),
            ColumnType::Enum(choices) => {
                let error = ConversionError::new(&tree_value, format!("{kind:?}"));
                let index: usize = tree_value.try_into()?;
                if index >= choices.len() {
                    Err(error)?
                } else {
                    Ok(Value::String(choices[index].clone()))
                }
            }
        }
    }
}

impl From<NaiveDateTime> for Value {
    fn from(value: NaiveDateTime) -> Self {
        Self::Timestamp(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<isize> for Value {
    fn from(value: isize) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::Float(value as f64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl TryFrom<Value> for i64 {
    type Error = ConversionError;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Integer(n) => Ok(n),
            Value::Timestamp(dt) => Ok(dt.timestamp()),
            _ => Err(ConversionError::new(value, "i64")),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = ConversionError;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Float(n) => Ok(n),
            _ => Err(ConversionError::new(value, "f64")),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = ConversionError;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(s),
            _ => Err(ConversionError::new(value, "String")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_from_datetime() {
        let ts = NaiveDateTime::from_timestamp_opt(1234, 5678).unwrap();
        assert_eq!(Value::from(ts.clone()), Value::Timestamp(ts));
    }

    #[test]
    fn value_from_int() {
        let n = 42;
        assert_eq!(Value::from(n), Value::Integer(n as i64));

        let n = 42_u32;
        assert_eq!(Value::from(n), Value::Integer(n as i64));

        let n = 42_u64;
        assert_eq!(Value::from(n), Value::Integer(n as i64));

        let n = 42_isize;
        assert_eq!(Value::from(n), Value::Integer(n as i64));

        let n = 42_usize;
        assert_eq!(Value::from(n), Value::Integer(n as i64));

        let n = 42_i64;
        assert_eq!(Value::from(n), Value::Integer(n));
    }

    #[test]
    fn value_from_float() {
        let n = 42.314;
        assert_eq!(Value::from(n), Value::Float(n as f64));

        let n = 42.314_f64;
        assert_eq!(Value::from(n), Value::Float(n));
    }

    #[test]
    fn value_from_string() {
        let s = String::from("Hi Mom!");
        assert_eq!(Value::from(s.clone()), Value::String(s));

        let s = "Hi Mom!";
        assert_eq!(Value::from(s), Value::String(String::from(s)));
    }

    #[test]
    fn value_from_tree_value() -> Result<()> {
        let tree_value = TreeValue::Timestamp(1234);
        let value = Value::from_tree_value(&ColumnType::Timestamp, tree_value)?;
        assert_eq!(
            value,
            Value::Timestamp(NaiveDateTime::from_timestamp_opt(1234, 0).unwrap())
        );

        let tree_value = TreeValue::Integer(1234);
        let value = Value::from_tree_value(&ColumnType::Integer, tree_value)?;
        assert_eq!(value, Value::Integer(1234));

        let tree_value = TreeValue::Float(12.34);
        let value = Value::from_tree_value(&ColumnType::Float, tree_value)?;
        assert_eq!(value, Value::Float(12.34));

        let tree_value = TreeValue::String("Hi Mom!".into());
        let value = Value::from_tree_value(&ColumnType::String, tree_value)?;
        assert_eq!(value, Value::String("Hi Mom!".into()));

        let tree_value = TreeValue::Enum(1);
        let value = Value::from_tree_value(
            &ColumnType::Enum(vec![
                "Hello World!".into(),
                "Hi Mom!".into(),
                "Hello Dad, I'm in jail.".into(),
            ]),
            tree_value,
        )?;
        assert_eq!(value, Value::String("Hi Mom!".into()));

        Ok(())
    }
}
