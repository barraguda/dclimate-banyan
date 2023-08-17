use std::{collections::BTreeMap, str::FromStr};

use chrono::NaiveDateTime;
use dclimate_banyan::{DataDefinition, IpfsStore, MemStore, Value};
use libipld::{multibase::Base, Cid};
use pyo3::{
    exceptions::{PyKeyError, PyNotImplementedError, PyValueError},
    prelude::*,
    pyclass::CompareOp,
    types::{IntoPyDict, PyDict, PySequence},
};

const TIMESTAMP: u8 = 0;
const INTEGER: u8 = 1;
const FLOAT: u8 = 2;
const STRING: u8 = 3;
const ENUM: u8 = 4;

#[pyfunction]
/// Checks to see if there is an IPFS node running locally.
///
/// The IPFS store can only be used if this function returns `True`. Otherwise, a memory
/// store must be used which is really only useful for testing.
///
/// Returns:
///     `True` if IPFS is available locally, `False` otherwise.
///
pub fn ipfs_available() -> bool {
    dclimate_banyan::ipfs_available()
}

#[pyclass]
pub struct PyStore(StoreInner);

#[derive(Clone)]
enum StoreInner {
    Ipfs(dclimate_banyan::IpfsStore),
    Memory(dclimate_banyan::MemStore),
}

#[pymethods]
impl PyStore {
    pub fn __repr__(&self) -> &str {
        match &self.0 {
            StoreInner::Ipfs(_) => "IpfsStore",
            StoreInner::Memory(_) => "MemStore",
        }
    }
}

#[pyfunction]
/// Creates and returns a Store which uses a locally running IPFS node
///
/// Returns:
///     An IPFS Store implementation.
///
pub fn ipfs_store() -> PyStore {
    PyStore(StoreInner::Ipfs(IpfsStore))
}

#[pyfunction]
/// Creates and returns a Store which uses local memory
///
/// This is convenient for testing but otherwise not particularly useful, as any data written is
/// lost whenever the program ends.
///
/// Returns:
///     An in-memory store implementation.
///
pub fn memory_store(max_size: usize) -> PyStore {
    PyStore(StoreInner::Memory(dclimate_banyan::memory_store(max_size)))
}

#[pyfunction]
/// Initialize a new, empty datastream.
///
/// Args:
///     store (Store): A store object such as that returned from :func:`ipfs_store` or
///         :func:`memory_store`.
///     data_definition (DataDefinition): The data definition for the data to be stored in the
///         datastream.
///
/// Returns:
///     :class:`Datastream`: An empty datastream, ready to receive data.
///
pub fn new_datastream(store: &PyStore, dd: &PyDataDefinition) -> PyDatastream {
    match &store.0 {
        StoreInner::Ipfs(store) => {
            let datastream = dclimate_banyan::Datastream::new(store.clone(), dd.0.clone());
            PyDatastream::wrap(DatastreamInner::Ipfs(datastream))
        }
        StoreInner::Memory(store) => {
            let datastream = dclimate_banyan::Datastream::new(store.clone(), dd.0.clone());
            PyDatastream::wrap(DatastreamInner::Memory(datastream))
        }
    }
}

#[pyfunction]
/// Loads a datastream from the given store with the given content identifier (cid).
///
/// Args:
///     cid (str): The content identifier of the datastream to retreive.
///
///     store (Store): A store object such as that returned from :func:`ipfs_store` or
///         :func:`memory_store`.
///     data_definition (DataDefinition): The data definition for the data to be stored in the
///         datastream.
///
/// Returns:
///     :class:`Datastream`: The loaded datastream. The datastram is ready to be read, queried, or
///         appended to.
///
pub fn load_datastream(
    cid: &str,
    store: &PyStore,
    dd: &PyDataDefinition,
) -> Result<PyDatastream, PyCidError> {
    let cid = Cid::from_str(cid)?;

    match &store.0 {
        StoreInner::Ipfs(store) => {
            let datastream = dclimate_banyan::Datastream::load(&cid, store.clone(), dd.0.clone());
            Ok(PyDatastream::wrap(DatastreamInner::Ipfs(datastream)))
        }
        StoreInner::Memory(store) => {
            let datastream = dclimate_banyan::Datastream::load(&cid, store.clone(), dd.0.clone());
            Ok(PyDatastream::wrap(DatastreamInner::Memory(datastream)))
        }
    }
}

#[pyclass]
pub struct PyDatastream {
    inner: DatastreamInner,
    start: Option<u64>,
    end: Option<u64>,
}

#[derive(Clone)]
enum DatastreamInner {
    Ipfs(dclimate_banyan::Datastream<IpfsStore>),
    Memory(dclimate_banyan::Datastream<MemStore>),
}

impl PyDatastream {
    fn wrap(inner: DatastreamInner) -> Self {
        Self {
            inner,
            start: None,
            end: None,
        }
    }
}

#[pymethods]
impl PyDatastream {
    #[getter]
    pub fn cid(&self) -> Option<String> {
        let cid = match &self.inner {
            DatastreamInner::Ipfs(ds) => ds.cid,
            DatastreamInner::Memory(ds) => ds.cid,
        };

        cid.map(|cid| cid.to_string_of_base(Base::Base32Lower).unwrap())
    }

    pub fn extend(&self, pyrecords: &PySequence) -> PyResult<Self> {
        if self.start.is_some() || self.end.is_some() {
            return Err(PyNotImplementedError::new_err("Cannot extend a slice"));
        }

        match &self.inner {
            DatastreamInner::Ipfs(datastream) => {
                let records = pyrecords
                    .iter()?
                    .map(|o| {
                        let pyrecord: Record = o?.extract()?;
                        let record = dclimate_banyan::Record::new(
                            &datastream.data_definition,
                            pyrecord.values,
                        );

                        Ok(record)
                    })
                    .collect::<dclimate_banyan::Result<Vec<dclimate_banyan::Record>>>()?;

                let datastream = datastream.clone().extend(records)?;
                Ok(Self::wrap(DatastreamInner::Ipfs(datastream)))
            }
            DatastreamInner::Memory(datastream) => {
                let records = pyrecords
                    .iter()?
                    .map(|o| {
                        let pyrecord: Record = o?.extract()?;
                        let record = dclimate_banyan::Record::new(
                            &datastream.data_definition,
                            pyrecord.values,
                        );

                        Ok(record)
                    })
                    .collect::<dclimate_banyan::Result<Vec<dclimate_banyan::Record>>>()?;

                let datastream = datastream.clone().extend(records)?;
                Ok(Self::wrap(DatastreamInner::Memory(datastream)))
            }
        }
    }

    pub fn collect(&self) -> PyResult<Vec<Record>> {
        match &self.inner {
            DatastreamInner::Ipfs(datastream) => {
                let view = dclimate_banyan::DatastreamView::new(datastream, self.start, self.end);
                let records = view
                    .iter()?
                    .map(|r| r.map(Record::wrap))
                    .collect::<dclimate_banyan::Result<Vec<Record>>>()?;

                Ok(records)
            }
            DatastreamInner::Memory(datastream) => {
                let view = dclimate_banyan::DatastreamView::new(datastream, self.start, self.end);
                let records = view
                    .iter()?
                    .map(|r| r.map(Record::wrap))
                    .collect::<dclimate_banyan::Result<Vec<Record>>>()?;

                Ok(records)
            }
        }
    }

    pub fn query(&self, query: &Query) -> PyResult<Vec<Record>> {
        match &self.inner {
            DatastreamInner::Ipfs(datastream) => {
                let view = dclimate_banyan::DatastreamView::new(datastream, self.start, self.end);
                let records = view
                    .query(&query.0)?
                    .map(|r| r.map(Record::wrap))
                    .collect::<dclimate_banyan::Result<Vec<Record>>>()?;

                Ok(records)
            }
            DatastreamInner::Memory(datastream) => {
                let view = dclimate_banyan::DatastreamView::new(datastream, self.start, self.end);
                let records = view
                    .query(&query.0)?
                    .map(|r| r.map(Record::wrap))
                    .collect::<dclimate_banyan::Result<Vec<Record>>>()?;

                Ok(records)
            }
        }
    }

    pub fn slice(&self, start: Option<u64>, end: Option<u64>) -> Self {
        let start = match self.start {
            Some(prev_start) => match start {
                Some(new_start) => Some(prev_start + new_start),
                None => Some(prev_start),
            },
            None => start,
        };

        let end = match self.start {
            Some(prev_start) => match end {
                Some(new_end) => Some(prev_start + new_end),
                None => self.end,
            },
            None => end,
        };

        Self {
            inner: self.inner.clone(),
            start,
            end,
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

    pub fn record(&self) -> Record {
        Record::new(self.0.clone())
    }

    pub fn get_by_name(&self, name: &str) -> PyResult<PyColumnDefinition> {
        match self.0.get_by_name(name) {
            Some(column) => Ok(PyColumnDefinition(column.clone())),
            None => Err(PyKeyError::new_err(String::from(name))),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    pub fn columns(&self) -> Vec<PyColumnDefinition> {
        self.0
            .columns()
            .map(|col| PyColumnDefinition(col.clone()))
            .collect()
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
    fn __richcmp__(&self, other: PyValue, op: CompareOp) -> PyResult<Query> {
        Ok(match op {
            CompareOp::Lt => Query(self.0.lt(other.into())?),
            CompareOp::Le => Query(self.0.le(other.into())?),
            CompareOp::Eq => Query(self.0.eq(other.into())?),
            CompareOp::Ne => Query(self.0.ne(other.into())?),
            CompareOp::Gt => Query(self.0.gt(other.into())?),
            CompareOp::Ge => Query(self.0.ge(other.into())?),
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.0.name.clone()
    }

    #[getter(type)]
    fn kind(&self, py: Python) -> PyObject {
        match &self.0.kind {
            dclimate_banyan::ColumnType::Timestamp => TIMESTAMP.into_py(py),
            dclimate_banyan::ColumnType::Integer => INTEGER.into_py(py),
            dclimate_banyan::ColumnType::Float => FLOAT.into_py(py),
            dclimate_banyan::ColumnType::String => STRING.into_py(py),
            dclimate_banyan::ColumnType::Enum(options) => options.clone().into_py(py),
        }
    }

    #[getter]
    fn index(&self) -> bool {
        self.0.index
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

#[pyclass]
/// An object representing a query of a Datastream.
///
/// A `Query` object is created when a :class:`ColumnDefinition` is compared to a value.
///
/// .. doctest::
///     :pyversion: ~= 3.9
///
///     >>> timestamp = data_definition["ts"]
///     >>> timestamp >= datetime(2010, 5, 12, 2, 42)
///     Query { clauses: [QueryAnd { clauses: [QueryCol { operator: GE, position: 0, value: Timestamp(1273632120) }] }] }
///
/// Queries can be combined using AND and OR logic using the bitwise `&` (and) and `|` (or)
/// operators. (The boolean operators `or` and `and` cannot be overridden in Python.)
///
///     >>> (timestamp >= datetime(2010, 5, 12, 2, 42)) & (timestamp < datetime(2015, 5, 12, 2, 42))
///     Query { clauses: [QueryAnd { clauses: [QueryCol { operator: GE, position: 0, value: Timestamp(1273632120) }, QueryCol { operator: LT, position: 0, value: Timestamp(1431398520) }] }] }
///     >>> (data_definition["one"] == 42) | (data_definition["two"] == 3.141592)
///     Query { clauses: [QueryAnd { clauses: [QueryCol { operator: EQ, position: 1, value: Integer(42) }] }, QueryAnd { clauses: [QueryCol { operator: EQ, position: 2, value: Float(3.141592) }] }] }
///
/// `Query` objects are passed to :meth:`Datastream.query` in order to query a datastream.
///
/// .. doctest::
///     :pyversion: ~= 3.9
///
///     >>> dec_2001 = (
///     ...     (timestamp >= datetime(2001, 12, 1, 0, 0)) &
///     ...     (timestamp < datetime(2002, 1, 1, 0, 0))
///     ... )
///     >>> query = dec_2001 & (data_definition["two"] > 2290.0)
///     >>> results = datastream.query(query)
///     >>> [record.as_dict() for record in results]
///     [{'one': 729, 'ts': datetime.datetime(2001, 12, 30, 0, 0), 'two': 2290.220568}, {'one': 730, 'ts': datetime.datetime(2001, 12, 31, 0, 0), 'two': 2293.36216}]
pub struct Query(dclimate_banyan::Query);

#[pymethods]
impl Query {
    fn __or__(&self, other: &Self) -> Self {
        Query(self.0.clone().or(other.0.clone()))
    }

    fn __and__(&self, other: &Self) -> Self {
        Query(self.0.clone().and(other.0.clone()))
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

fn value_to_py(value: &Value, py: Python) -> PyObject {
    match value {
        Value::Timestamp(value) => value.into_py(py),
        Value::Integer(value) => value.into_py(py),
        Value::Float(value) => value.into_py(py),
        Value::String(value) => value.into_py(py),
    }
}

#[pyclass(mapping)]
#[derive(Clone, Debug, PartialEq)]
/// Represents a single row of data in a datastream.
///
/// A datastream is composed of and retrieves instances of `Record`. A `Record` instance is
/// backed by the :class:`DataDefinition` of the associated :class:`Datastream`. Use the
/// :class:`DataDefinition` to create a new record.
///
/// .. doctest::
///     :pyversion: ~= 3.9
///
///     >>> record = data_definition.record()
///     >>> record
///     Record { data_definition: DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "one", kind: Integer, index: false }, ColumnDefinition { position: 2, name: "two", kind: Float, index: true }, ColumnDefinition { position: 3, name: "three", kind: String, index: false }, ColumnDefinition { position: 4, name: "four", kind: Enum(["foo", "bar", "baz"]), index: false }]), values: {} }
///
/// Fields on records may be set and read using regular item syntax.
///
/// .. doctest::
///     :pyversion: ~= 3.9
///
///     >>> record["one"] = 42
///     >>> record["one"]
///     42
///     >>> del record["one"]
///     >>> record["one"]
///     Traceback (most recent call last):
///       ...
///     KeyError: 'one'
///
/// `Record` doesn't fully support the Python `dict` interface, but if you want to use `dict`
/// methods you can convert a `Record` to a `dict` using :meth:`Record.as_dict`.
///
/// .. doctest::
///     :pyversion: ~= 3.9
///
///    >>> record = data_definition.record({"one": 56, "two": 2.71828})
///    >>> record.as_dict()
///    {'one': 56, 'two': 2.71828}
pub struct Record {
    data_definition: dclimate_banyan::DataDefinition,
    values: BTreeMap<String, dclimate_banyan::Value>,
}

impl Record {
    fn new(data_definition: DataDefinition) -> Self {
        let values = BTreeMap::new();

        Self {
            data_definition,
            values,
        }
    }

    fn wrap(record: dclimate_banyan::Record) -> Self {
        Self {
            data_definition: record.data_definition.clone(),
            values: record.values,
        }
    }
}

#[pymethods]
impl Record {
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

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    /// Returns the values held by this record as a regular Python dictionary.
    ///
    /// Returns:
    ///     A Python dictionary using column names as keys with values from this record.
    ///
    fn as_dict<'py>(&self, py: Python<'py>) -> &'py PyDict {
        self.values
            .iter()
            .map(|(key, value)| (key.into_py(py), value_to_py(value, py)))
            .into_py_dict(py)
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
    m.add_function(wrap_pyfunction!(ipfs_store, m)?)?;
    m.add_function(wrap_pyfunction!(load_datastream, m)?)?;
    m.add_function(wrap_pyfunction!(memory_store, m)?)?;
    m.add_function(wrap_pyfunction!(new_datastream, m)?)?;

    // Column types
    m.add("Timestamp", TIMESTAMP)?;
    m.add("Integer", INTEGER)?;
    m.add("Float", FLOAT)?;
    m.add("String", STRING)?;
    m.add("Enum", ENUM)?;

    m.add_class::<PyDataDefinition>()?;
    m.add_class::<Query>()?;
    m.add_class::<Record>()?;

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
