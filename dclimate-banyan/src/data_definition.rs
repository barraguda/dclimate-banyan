use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use crate::{
    bitmap::Bitmap,
    codec::{Row, TreeKey, TreeValue},
    error::Result,
    Value,
};

#[derive(PartialEq, Debug)]
pub struct DataDefinition(pub Vec<ColumnDefinition>);

#[derive(PartialEq, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub kind: ColumnType,
    pub index: bool,
}

impl ColumnDefinition {
    pub fn new<S: Into<String>>(name: S, kind: ColumnType, index: bool) -> Self {
        let name = name.into();

        Self { name, kind, index }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Timestamp = 0,
    Integer = 1,
    Float = 2,
    String = 3,
    Enum(Vec<&'static str>) = 4,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record<'dd> {
    data_definition: &'dd DataDefinition,
    values: BTreeMap<String, Value>,
}

impl DataDefinition {
    pub fn record<'dd>(&'dd self) -> Record<'dd> {
        Record {
            data_definition: self,
            values: BTreeMap::new(),
        }
    }

    pub(crate) fn row_from_record(&self, mut record: Record) -> Result<Row> {
        let mut columns = Bitmap::new();
        let mut values = Vec::new();
        for (index, column) in self.0.iter().enumerate() {
            match record.values.remove(column.name.as_str()) {
                Some(value) => {
                    columns.set(index, true);
                    values.push(TreeValue::from_value(&column.kind, value)?);
                }
                None => {
                    columns.set(index, false);
                }
            }
        }

        Ok(Row(columns, values))
    }

    pub(crate) fn key_from_record(&self, _r: &Record) -> TreeKey {
        ()
    }

    pub(crate) fn record_from_row(&self, row: Row) -> Result<Record> {
        let mut values = row.1.into_iter();
        let mut dict = BTreeMap::new();
        for (index, column) in self.0.iter().enumerate() {
            if row.0.get(index) {
                dict.insert(
                    column.name.clone(),
                    Value::from_tree_value(&column.kind, values.next().unwrap())?,
                );
            }
        }

        Ok(Record {
            data_definition: self,
            values: dict,
        })
    }
}

impl<'dd> Deref for Record<'dd> {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'dd> DerefMut for Record<'dd> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_datastream() -> DataDefinition {
        let mut cols = vec![];
        cols.push(ColumnDefinition::new("one", ColumnType::Integer, true));

        DataDefinition(cols)
    }
    #[test]
    fn new_column_definition() {
        let coldef = ColumnDefinition::new("strref", ColumnType::Integer, true);
        assert_eq!(coldef.name, String::from("strref"));
        assert_eq!(coldef.kind, ColumnType::Integer);
        assert!(coldef.index);

        let coldef = ColumnDefinition::new(String::from("owned"), ColumnType::Float, false);
        assert_eq!(coldef.name, String::from("owned"));
        assert_eq!(coldef.kind, ColumnType::Float);
        assert!(!coldef.index);
    }

    #[test]
    fn new_record() {
        let dd = make_datastream();
        let record = dd.record();

        assert!(record.data_definition as *const _ == &dd as *const _);
    }
}
