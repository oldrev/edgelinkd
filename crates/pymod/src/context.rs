//! Python bindings for the context stores.
//!
//! The pytest suite mirrors Node-RED's own context-storage specs, which drive a store directly
//! rather than through a flow. This exposes the Rust [`ContextStore`] to Python so those specs
//! can be ported one `it()` at a time, on the store the runtime actually uses.

use std::{str::FromStr, sync::Arc};

use edgelink_core::EdgelinkError;
use edgelink_core::runtime::context::{ContextStore, ContextStoreOptions, create_context_store};
use edgelink_core::runtime::model::{ElementId, Variant, propex};
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use crate::json;

/// Map a core error onto the Python one that matches its meaning.
///
/// The stores signal "no such value" with [`EdgelinkError::OutOfRange`] — the same thing
/// Node-RED reports as `undefined` — which is a `KeyError` here. Anything else is a real
/// failure, such as a corrupt context file, and surfaces as a `RuntimeError`.
fn to_py_err(err: anyhow::Error) -> PyErr {
    if matches!(err.downcast_ref::<EdgelinkError>(), Some(EdgelinkError::OutOfRange)) {
        PyKeyError::new_err("The context key is not set")
    } else {
        PyRuntimeError::new_err(err.to_string())
    }
}

fn serde_err(err: serde_json::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Convert a Python value into the message model's [`Variant`].
///
/// `None` becomes JSON `null`, which the stores keep as a real value; deleting a key goes
/// through [`PyContextStore::remove`] instead. A value the message model cannot represent is
/// rejected rather than quietly turned into `null`.
fn py_to_variant(obj: &Bound<'_, PyAny>) -> PyResult<Variant> {
    if obj.is_none() {
        return Ok(Variant::Null);
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(py_to_variant(&item)?);
        }
        return Ok(Variant::Array(items));
    }
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            items.push(py_to_variant(&item)?);
        }
        return Ok(Variant::Array(items));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = edgelink_core::runtime::model::VariantObjectMap::new();
        for (key, value) in dict.iter() {
            map.insert(key.extract::<String>()?, py_to_variant(&value)?);
        }
        return Ok(Variant::Object(map));
    }
    // `bool` is a subclass of `int` in Python, so it has to be tested for first.
    if let Ok(boolean) = obj.downcast::<PyBool>() {
        return Ok(Variant::from(boolean.is_true()));
    }
    if let Ok(int) = obj.downcast::<PyInt>() {
        return Ok(Variant::from(int.extract::<i64>()?));
    }
    if let Ok(float) = obj.downcast::<PyFloat>() {
        return Ok(Variant::from(float.extract::<f64>()?));
    }
    if let Ok(string) = obj.downcast::<PyString>() {
        return Ok(Variant::from(string.extract::<String>()?));
    }
    Err(PyTypeError::new_err(format!("Unsupported context value of type '{}'", obj.get_type().name()?)))
}

fn variant_to_py(py: Python<'_>, value: &Variant) -> PyResult<PyObject> {
    json::json_value_to_py_object(py, &serde_json::to_value(value).map_err(serde_err)?)
}

/// A context store, constructible from a provider name and its options.
///
/// Every method is a coroutine; see `tests/context/test_localfilesystem_store.py` for the
/// behaviour each one is expected to have.
#[pyclass(name = "ContextStore", module = "edgelink_pymod")]
pub struct PyContextStore {
    store: Arc<dyn ContextStore>,
}

#[pymethods]
impl PyContextStore {
    /// `ContextStore.create(provider, name, options=None)` — build one store.
    ///
    /// `options` is the provider's own configuration object, the same keys the flat
    /// `[runtime.context.stores]` table holds in `edgelinkd.toml`.
    #[staticmethod]
    #[pyo3(signature = (provider, name, options=None))]
    fn create(provider: &str, name: &str, options: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let options = match options {
            Some(options) => json::py_object_to_json_value(options)?,
            None => serde_json::Value::Null,
        };
        let options = ContextStoreOptions::from_json_options(provider, options)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let store = create_context_store(name, &options).map_err(to_py_err)?;
        Ok(Self { store: Arc::from(store) })
    }

    /// Load whatever the store persists, and create its storage area.
    fn open<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.open().await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Flush and release the store.
    fn close<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.close().await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Read one property, by path expression. Raises `KeyError` when it is not set.
    fn get<'a>(&self, py: Python<'a>, scope: String, key: String) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let path = propex::parse(&key).map_err(|e| PyValueError::new_err(e.to_string()))?;
            let value = store.get_one(&scope, &path).await.map_err(to_py_err)?;
            Python::with_gil(|py| variant_to_py(py, &value))
        })
    }

    /// Read several properties at once; a property that is not set comes back as `None`.
    fn get_many<'a>(&self, py: Python<'a>, scope: String, keys: Vec<String>) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let borrowed: Vec<&str> = keys.iter().map(String::as_str).collect();
            let values = store.get_many(&scope, &borrowed).await.map_err(to_py_err)?;
            Python::with_gil(|py| {
                let list = PyList::empty(py);
                for value in &values {
                    list.append(variant_to_py(py, value)?)?;
                }
                Ok(list.into_any().unbind())
            })
        })
    }

    /// Write one property, by path expression. `None` is stored as JSON `null`.
    fn set<'a>(
        &self,
        py: Python<'a>,
        scope: String,
        key: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        let value = py_to_variant(value)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let path = propex::parse(&key).map_err(|e| PyValueError::new_err(e.to_string()))?;
            store.set_one(&scope, &path, value).await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Write several properties in one operation.
    ///
    /// Mirrors Node-RED's array form of `set`: when `values` is not a list only the first key
    /// takes it, and the remaining keys are set to JSON `null`.
    fn set_many<'a>(
        &self,
        py: Python<'a>,
        scope: String,
        keys: Vec<String>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        let mut values: Vec<Variant> = match values.downcast::<PyList>() {
            Ok(list) => list.iter().map(|item| py_to_variant(&item)).collect::<PyResult<_>>()?,
            Err(_) => vec![py_to_variant(values)?],
        };
        // Node-RED pads a short value list with `null` rather than skipping the key.
        values.resize(keys.len().max(values.len()), Variant::Null);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let pairs = keys.into_iter().zip(values).collect();
            store.set_many(&scope, pairs).await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Remove one property and return the value it held.
    fn remove<'a>(&self, py: Python<'a>, scope: String, key: String) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let path = propex::parse(&key).map_err(|e| PyValueError::new_err(e.to_string()))?;
            let value = store.remove_one(&scope, &path).await.map_err(to_py_err)?;
            Python::with_gil(|py| variant_to_py(py, &value))
        })
    }

    /// Enumerate the top-level keys of a scope.
    fn keys<'a>(&self, py: Python<'a>, scope: String) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let keys = store.get_keys(&scope).await.map_err(to_py_err)?;
            Python::with_gil(|py| Ok(PyList::new(py, keys)?.into_any().unbind()))
        })
    }

    /// Drop a whole scope.
    fn delete<'a>(&self, py: Python<'a>, scope: String) -> PyResult<Bound<'a, PyAny>> {
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.delete(&scope).await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Drop every scope that does not belong to one of `active_nodes`.
    ///
    /// The ids are the runtime's element ids, the 16-digit hex strings a scope is built from.
    fn clean<'a>(&self, py: Python<'a>, active_nodes: Vec<String>) -> PyResult<Bound<'a, PyAny>> {
        let mut active = Vec::with_capacity(active_nodes.len());
        for name in active_nodes {
            let id = ElementId::from_str(&name)
                .map_err(|_| PyValueError::new_err(format!("Not a valid node or flow id: '{name}'")))?;
            active.push(id);
        }
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.clean(&active).await.map_err(to_py_err)?;
            Ok(())
        })
    }
}
