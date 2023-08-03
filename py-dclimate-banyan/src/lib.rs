use std::{collections::BTreeMap, str::FromStr};

use banyan::store::MemStore;
use banyan_utils::tags::Sha256Digest;
use chrono::NaiveDateTime;
use dclimate_banyan::{DataDefinition, IpfsStore, Record, Value};
use libipld::{multibase::Base, Cid};
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    pyclass::CompareOp,
    types::PySequence,
};

#[pyfunction]
pub fn ipfs_available() -> bool {
    dclimate_banyan::ipfs_available()
}

#[pyclass]
pub struct PyResolver(ResolverInner);

#[derive(Clone)]
enum ResolverInner {
    Ipfs(dclimate_banyan::Resolver<IpfsStore>),
    Memory(dclimate_banyan::Resolver<MemStore<Sha256Digest>>),
}

#[pyfunction]
pub fn ipfs_resolver() -> PyResolver {
    let resolver = dclimate_banyan::Resolver::new(IpfsStore);

    PyResolver(ResolverInner::Ipfs(resolver))
}

#[pyfunction]
pub fn memory_resolver(max_size: usize) -> PyResolver {
    let store = dclimate_banyan::memory_store(max_size);
    let resolver = dclimate_banyan::Resolver::new(store);

    PyResolver(ResolverInner::Memory(resolver))
}

#[pymethods]
impl PyResolver {
    pub fn new_datastream(&self, dd: &PyDataDefinition) -> PyDatastream {
        let resolver = self.0.clone();
        let data_definition = dd.0.clone();

        PyDatastream {
            cid: None,
            data_definition,
            resolver,
        }
    }

    pub fn load_datastream(
        &self,
        data_definition: &PyDataDefinition,
        cid: &str,
    ) -> Result<PyDatastream, PyCidError> {
        let cid = Cid::from_str(cid)?;

        Ok(PyDatastream {
            cid: Some(cid),
            data_definition: data_definition.0.clone(),
            resolver: self.0.clone(),
        })
    }
}

#[pyclass]
pub struct PyDatastream {
    cid: Option<Cid>,
    data_definition: dclimate_banyan::DataDefinition,
    resolver: ResolverInner,
}

#[pymethods]
impl PyDatastream {
    #[getter]
    pub fn cid(&self) -> Option<String> {
        match &self.cid {
            Some(cid) => Some(cid.to_string_of_base(Base::Base32Lower).unwrap()),
            None => None,
        }
    }

    pub fn extend(&self, pyrecords: &PySequence) -> PyResult<Self> {
        let cid = match &self.resolver {
            ResolverInner::Ipfs(resolver) => {
                let datastream = match self.cid {
                    None => resolver.new_datastream(&self.data_definition),
                    Some(cid) => resolver.load_datastream(&cid, &self.data_definition),
                };
                let records = pyrecords
                    .iter()?
                    .map(|o| {
                        let pyrecord: PyRecord = o?.extract()?;
                        let record = Record::new(&self.data_definition, pyrecord.values);

                        Ok(record)
                    })
                    .collect::<dclimate_banyan::Result<Vec<Record>>>()?;
                datastream.extend(records)?.cid.unwrap()
            }
            ResolverInner::Memory(resolver) => {
                let datastream = match self.cid {
                    None => resolver.new_datastream(&self.data_definition),
                    Some(cid) => resolver.load_datastream(&cid, &self.data_definition),
                };
                let records = pyrecords
                    .iter()?
                    .map(|o| {
                        let pyrecord: PyRecord = o?.extract()?;
                        let record = Record::new(&self.data_definition, pyrecord.values);

                        Ok(record)
                    })
                    .collect::<dclimate_banyan::Result<Vec<Record>>>()?;
                datastream.extend(records)?.cid.unwrap()
            }
        };

        Ok(Self {
            cid: Some(cid),
            data_definition: self.data_definition.clone(),
            resolver: self.resolver.clone(),
        })
    }

    pub fn collect(&self) -> PyResult<Vec<PyRecord>> {
        match self.cid {
            None => Ok(vec![]),
            Some(cid) => match &self.resolver {
                ResolverInner::Ipfs(resolver) => {
                    let datastream = resolver.load_datastream(&cid, &self.data_definition);
                    let records = datastream
                        .iter()?
                        .map(|r| r.map(PyRecord::wrap))
                        .collect::<dclimate_banyan::Result<Vec<PyRecord>>>()?;

                    Ok(records)
                }
                ResolverInner::Memory(resolver) => {
                    let datastream = resolver.load_datastream(&cid, &self.data_definition);
                    let records = datastream
                        .iter()?
                        .map(|r| r.map(PyRecord::wrap))
                        .collect::<dclimate_banyan::Result<Vec<PyRecord>>>()?;

                    Ok(records)
                }
            },
        }
    }

    pub fn query(&self, query: &PyQuery) -> PyResult<Vec<PyRecord>> {
        match self.cid {
            None => Ok(vec![]),
            Some(cid) => match &self.resolver {
                ResolverInner::Ipfs(resolver) => {
                    let datastream = resolver.load_datastream(&cid, &self.data_definition);
                    let records = datastream
                        .query(&query.0)?
                        .map(|r| r.map(PyRecord::wrap))
                        .collect::<dclimate_banyan::Result<Vec<PyRecord>>>()?;

                    Ok(records)
                }
                ResolverInner::Memory(resolver) => {
                    let datastream = resolver.load_datastream(&cid, &self.data_definition);
                    let records = datastream
                        .query(&query.0)?
                        .map(|r| r.map(PyRecord::wrap))
                        .collect::<dclimate_banyan::Result<Vec<PyRecord>>>()?;

                    Ok(records)
                }
            },
        }
    }
}

#[pyclass]
pub struct PyDataDefinition(dclimate_banyan::DataDefinition);

#[pymethods]
impl PyDataDefinition {
    #[new]
    pub fn new(columns: Vec<ColumnSpec>) -> Self {
        let dd = dclimate_banyan::DataDefinition::from_iter(columns.into_iter().map(
            |(name, kind, indexed)| {
                let kind = match kind {
                    PyColumnType::NonEnum(kind) => match kind {
                        0 => dclimate_banyan::ColumnType::Timestamp,
                        1 => dclimate_banyan::ColumnType::Integer,
                        2 => dclimate_banyan::ColumnType::Float,
                        3 => dclimate_banyan::ColumnType::String,
                        _ => panic!("Bad column type: {kind}"),
                    },
                    PyColumnType::Enum(options) => dclimate_banyan::ColumnType::Enum(options),
                };

                (name, kind, indexed)
            },
        ));

        Self(dd)
    }

    pub fn record(&self) -> PyRecord {
        PyRecord::new(self.0.clone())
    }

    pub fn get_by_name(&self, name: &str) -> PyResult<PyColumnDefinition> {
        match self.0.get_by_name(name) {
            Some(column) => Ok(PyColumnDefinition(column.clone())),
            None => Err(PyKeyError::new_err(String::from(name))),
        }
    }
}

pub type ColumnSpec = (String, PyColumnType, bool);

#[derive(FromPyObject)]
pub enum PyColumnType {
    NonEnum(u8),
    Enum(Vec<String>),
}

#[derive(Clone, FromPyObject)]
pub enum PyValue {
    Timestamp(NaiveDateTime),
    Integer(i64),
    Float(f64),
    String(String),
}

impl From<PyValue> for dclimate_banyan::Value {
    fn from(value: PyValue) -> Self {
        match value {
            PyValue::Timestamp(ts) => Value::Timestamp(ts),
            PyValue::Integer(n) => Value::Integer(n),
            PyValue::Float(n) => Value::Float(n),
            PyValue::String(s) => Value::String(s),
        }
    }
}

#[pyclass]
pub struct PyColumnDefinition(dclimate_banyan::ColumnDefinition);

#[pymethods]
impl PyColumnDefinition {
    fn __richcmp__(&self, other: PyValue, op: CompareOp) -> PyResult<PyQuery> {
        Ok(match op {
            CompareOp::Lt => PyQuery(self.0.lt(other.into())?),
            CompareOp::Le => PyQuery(self.0.le(other.into())?),
            CompareOp::Eq => PyQuery(self.0.eq(other.into())?),
            CompareOp::Ne => PyQuery(self.0.ne(other.into())?),
            CompareOp::Gt => PyQuery(self.0.gt(other.into())?),
            CompareOp::Ge => PyQuery(self.0.ge(other.into())?),
        })
    }
}

#[pyclass]
pub struct PyQuery(dclimate_banyan::Query);

#[pymethods]
impl PyQuery {
    fn __or__(&self, other: &Self) -> Self {
        PyQuery(self.0.clone().or(other.0.clone()))
    }

    fn __and__(&self, other: &Self) -> Self {
        PyQuery(self.0.clone().and(other.0.clone()))
    }
}

#[pyclass(mapping)]
#[derive(Clone, PartialEq)]
pub struct PyRecord {
    data_definition: dclimate_banyan::DataDefinition,
    values: BTreeMap<String, dclimate_banyan::Value>,
}

impl PyRecord {
    fn new(data_definition: DataDefinition) -> Self {
        let values = BTreeMap::new();

        Self {
            data_definition,
            values,
        }
    }

    fn wrap(record: Record) -> Self {
        Self {
            data_definition: record.data_definition.clone(),
            values: record.values,
        }
    }
}

#[pymethods]
impl PyRecord {
    fn __getitem__(&self, key: String, py: Python) -> PyResult<PyObject> {
        match self.values.get(&key) {
            None => Err(PyKeyError::new_err(key)),
            Some(value) => Ok(match value {
                Value::Timestamp(value) => value.into_py(py),
                Value::Integer(value) => value.into_py(py),
                Value::Float(value) => value.into_py(py),
                Value::String(value) => value.into_py(py),
            }),
        }
    }

    fn __setitem__(&mut self, py: Python, key: String, value: PyObject) -> PyResult<()> {
        let value = value_from_py_object(py, value)?;
        self.values.insert(key, value);

        Ok(())
    }

    fn __delitem__(&mut self, key: String) -> PyResult<()> {
        self.values.remove(&key);

        Ok(())
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp, py: Python) -> PyObject {
        match op {
            CompareOp::Eq => (*self == *other).into_py(py),
            CompareOp::Ne => (*self != *other).into_py(py),
            _ => py.NotImplemented(),
        }
    }
}

fn value_from_py_object(py: Python, value: PyObject) -> PyResult<Value> {
    if let Ok(ts) = value.extract(py) {
        return Ok(Value::Timestamp(ts));
    } else if let Ok(n) = value.extract(py) {
        return Ok(Value::Integer(n));
    } else if let Ok(n) = value.extract(py) {
        return Ok(Value::Float(n));
    } else if let Ok(s) = value.extract(py) {
        return Ok(Value::String(s));
    }

    Err(PyValueError::new_err(format!(
        "Unable to convert {value:?} into Banyan value"
    )))
}

pub struct PyCidError(libipld::cid::Error);

impl From<PyCidError> for PyErr {
    fn from(error: PyCidError) -> Self {
        PyValueError::new_err(format!("{}", error.0))
    }
}

impl From<libipld::cid::Error> for PyCidError {
    fn from(error: libipld::cid::Error) -> Self {
        Self(error)
    }
}

#[pymodule]
fn _banyan(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ipfs_available, m)?)?;
    m.add_function(wrap_pyfunction!(ipfs_resolver, m)?)?;
    m.add_function(wrap_pyfunction!(memory_resolver, m)?)?;

    // Column types
    m.add("Timestamp", 0_u8)?;
    m.add("Integer", 1_u8)?;
    m.add("Float", 2_u8)?;
    m.add("String", 3_u8)?;

    m.add_class::<PyDataDefinition>()?;
    m.add_class::<PyRecord>()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_definition() -> dclimate_banyan::DataDefinition {
        dclimate_banyan::DataDefinition::from_iter(vec![(
            "one",
            dclimate_banyan::ColumnType::Integer,
            false,
        )])
    }

    #[test]
    fn test_column_richcmp() -> PyResult<()> {
        let dd = data_definition();
        let col = dd.get_by_name("one").unwrap();
        let pycol = PyColumnDefinition(col.clone());
        let val = dclimate_banyan::Value::Integer(42);
        let pyval = PyValue::Integer(42);

        assert_eq!(
            pycol.__richcmp__(pyval.clone(), CompareOp::Lt)?.0,
            col.lt(val.clone())?
        );
        assert_eq!(
            pycol.__richcmp__(pyval.clone(), CompareOp::Le)?.0,
            col.le(val.clone())?
        );
        assert_eq!(
            pycol.__richcmp__(pyval.clone(), CompareOp::Eq)?.0,
            col.eq(val.clone())?
        );
        assert_eq!(
            pycol.__richcmp__(pyval.clone(), CompareOp::Ne)?.0,
            col.ne(val.clone())?
        );
        assert_eq!(
            pycol.__richcmp__(pyval.clone(), CompareOp::Gt)?.0,
            col.gt(val.clone())?
        );
        assert_eq!(pycol.__richcmp__(pyval, CompareOp::Ge)?.0, col.ge(val)?);

        Ok(())
    }

    #[test]
    fn test_query_conjunctions() -> PyResult<()> {
        let dd = data_definition();
        let col = dd.get_by_name("one").unwrap();
        let pycol = PyColumnDefinition(col.clone());
        let query1 = pycol.__richcmp__(PyValue::Integer(42), CompareOp::Lt)?;
        let query2 = pycol.__richcmp__(PyValue::Integer(32), CompareOp::Gt)?;

        assert_eq!(
            query1.__or__(&query2).0,
            query1.0.clone().or(query2.0.clone())
        );
        assert_eq!(query1.__and__(&query2).0, query1.0.and(query2.0));

        Ok(())
    }
}
