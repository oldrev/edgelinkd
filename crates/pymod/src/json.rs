use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use serde_json::{Map, Value};

pub fn py_object_to_json_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut json_list = Vec::new();
        for item in list.iter() {
            json_list.push(py_object_to_json_value(&item)?);
        }
        Ok(Value::Array(json_list))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut json_map = Map::new();
        for (key, value) in dict.iter() {
            let key = key.extract::<String>()?;
            let value = py_object_to_json_value(&value)?;
            json_map.insert(key, value);
        }
        Ok(Value::Object(json_map))
    } else if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let mut json_list = Vec::new();
        for item in tuple.iter() {
            json_list.push(py_object_to_json_value(&item)?);
        }
        Ok(Value::Array(json_list))
    } else if let Ok(boolean) = obj.downcast::<PyBool>() {
        Ok(Value::Bool(boolean.extract::<bool>()?))
    } else if let Ok(float) = obj.downcast::<PyFloat>() {
        let num = float.extract::<f64>()?;
        Ok(serde_json::json!(num))
    } else if let Ok(int) = obj.downcast::<PyInt>() {
        let num = int.extract::<i64>()?;
        Ok(serde_json::json!(num))
    } else if let Ok(string) = obj.downcast::<PyString>() {
        Ok(Value::String(string.extract::<String>()?))
    } else {
        Ok(Value::Null)
    }
}

pub fn json_value_to_py_object(py: Python, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::Number(n) => {
            if let Some(int) = n.as_i64() {
                Ok(int.into_pyobject(py)?.into_any().unbind())
            } else if let Some(float) = n.as_f64() {
                Ok(PyFloat::new(py, float).into_any().unbind())
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid number type".to_string()))
            }
        }
        Value::String(s) => Ok(PyString::new(py, s).into_any().unbind()),
        Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                let py_item = json_value_to_py_object(py, item)?;
                list.append(py_item)?;
            }
            Ok(list.into_any().unbind())
        }
        Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (key, value) in obj {
                let py_key = PyString::new(py, key);
                let py_value = json_value_to_py_object(py, value)?;
                dict.set_item(py_key, py_value)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}
