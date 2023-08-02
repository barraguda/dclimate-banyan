use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use crate::{
    bitmap::Bitmap,
    codec::{Row, TreeKey, TreeValue},
    error::Result,
    query::{Query, EQ, GE, GT, LE, LT},
    value::Value,
};

#[derive(Clone, PartialEq, Debug)]
pub struct DataDefinition(Vec<ColumnDefinition>);

impl DataDefinition {
    pub fn columns(&self) -> impl Iterator<Item = &ColumnDefinition> {
        self.0.iter()
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ColumnDefinition> {
        for column in &self.0 {
            if column.name == name {
                return Some(column);
            }
        }

        None
    }

    pub fn record<'dd>(&'dd self) -> Record<'dd> {
        Record {
            data_definition: self,
            values: BTreeMap::new(),
        }
    }

    pub(crate) fn row_from_record(&self, record: Record) -> Result<Row> {
        self._row_from_record(record, false)
    }

    pub(crate) fn key_from_record(&self, record: Record) -> Result<TreeKey> {
        self._row_from_record(record, true)
    }

    pub(crate) fn _row_from_record(&self, mut record: Record, indexed: bool) -> Result<Row> {
        let mut columns = Bitmap::new();
        let mut values = Vec::new();
        for (index, column) in self.0.iter().enumerate() {
            if indexed && !column.index {
                columns.set(index, false);
            } else {
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
        }

        Ok(Row(columns, values))
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

impl<S> FromIterator<(S, ColumnType, bool)> for DataDefinition
where
    S: Into<String>,
{
    fn from_iter<T: IntoIterator<Item = (S, ColumnType, bool)>>(iter: T) -> Self {
        let mut dd = Self(Vec::new());
        let mut position = 0;
        for (name, kind, index) in iter {
            let column = ColumnDefinition {
                position,
                name: name.into(),
                kind,
                index,
            };
            dd.0.push(column);
            position += 1;
        }

        dd
    }
}

impl Deref for DataDefinition {
    type Target = Vec<ColumnDefinition>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ColumnDefinition {
    position: usize,
    pub name: String,
    pub kind: ColumnType,
    pub index: bool,
}

impl ColumnDefinition {
    pub fn lt(&self, value: Value) -> Result<Query> {
        Ok(Query::new(
            self.position,
            LT,
            TreeValue::from_value(&self.kind, value)?,
        ))
    }

    pub fn le(&self, value: Value) -> Result<Query> {
        Ok(Query::new(
            self.position,
            LE,
            TreeValue::from_value(&self.kind, value)?,
        ))
    }

    pub fn eq(&self, value: Value) -> Result<Query> {
        Ok(Query::new(
            self.position,
            EQ,
            TreeValue::from_value(&self.kind, value)?,
        ))
    }

    pub fn ge(&self, value: Value) -> Result<Query> {
        Ok(Query::new(
            self.position,
            GE,
            TreeValue::from_value(&self.kind, value)?,
        ))
    }

    pub fn gt(&self, value: Value) -> Result<Query> {
        Ok(Query::new(
            self.position,
            GT,
            TreeValue::from_value(&self.kind, value)?,
        ))
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Timestamp = 0,
    Integer = 1,
    Float = 2,
    String = 3,
    Enum(Vec<String>) = 4,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record<'dd> {
    pub data_definition: &'dd DataDefinition,
    pub values: BTreeMap<String, Value>,
}

impl<'dd> Record<'dd> {
    pub fn new(data_definition: &'dd DataDefinition, values: BTreeMap<String, Value>) -> Self {
        Self {
            data_definition,
            values,
        }
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
        cols.push(("one", ColumnType::Integer, true));

        DataDefinition::from_iter(cols)
    }

    #[test]
    fn new_record() {
        let dd = make_datastream();
        let record = dd.record();

        assert!(record.data_definition as *const _ == &dd as *const _);
    }

    #[test]
    fn col_cmp() -> Result<()> {
        let col = ColumnDefinition {
            position: 32,
            name: "test".into(),
            kind: ColumnType::Integer,
            index: true,
        };

        // TODO: Validation. Can't query non-indexed columns, query value must be same type as column

        let value = Value::Integer(42);
        assert_eq!(
            col.lt(value.clone())?,
            Query::new(32, LT, TreeValue::Integer(42))
        );
        assert_eq!(
            col.le(value.clone())?,
            Query::new(32, LE, TreeValue::Integer(42))
        );
        assert_eq!(
            col.eq(value.clone())?,
            Query::new(32, EQ, TreeValue::Integer(42))
        );
        assert_eq!(
            col.ge(value.clone())?,
            Query::new(32, GE, TreeValue::Integer(42))
        );
        assert_eq!(
            col.gt(value.clone())?,
            Query::new(32, GT, TreeValue::Integer(42))
        );

        Ok(())
    }
}
