use edgelink_core::runtime::model::{Msg, Variant};
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
        if dict.len() == 2 {
            let marker = dict.get_item("$__el_type")?;
            let data = dict.get_item("data")?;
            if marker.as_ref().and_then(|value| value.extract::<String>().ok()).as_deref() == Some("Buffer") {
                let data =
                    data.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Buffer tag requires data"))?;
                let values = data.downcast::<PyList>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Buffer data must be a list of bytes")
                })?;
                let mut bytes = Vec::with_capacity(values.len());
                for value in values.iter() {
                    if value.downcast::<PyBool>().is_ok() {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Buffer byte must be an integer from 0 to 255",
                        ));
                    }
                    bytes.push(value.extract::<u8>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Buffer byte must be an integer from 0 to 255")
                    })?);
                }
                return Ok(serde_json::json!({"type":"Buffer", "data":bytes, "__enc__":true, "length":bytes.len()}));
            }
        }
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

/// Encode runtime values for the Python flow bridge while preserving binary identity.
pub fn variant_to_bridge_json(value: &Variant) -> Value {
    match value {
        Variant::Bytes(bytes) => serde_json::json!({"$__el_type":"Buffer", "data":bytes}),
        Variant::Array(values) => Value::Array(values.iter().map(variant_to_bridge_json).collect()),
        Variant::Object(values) => {
            Value::Object(values.iter().map(|(key, value)| (key.clone(), variant_to_bridge_json(value))).collect())
        }
        _ => serde_json::to_value(value).unwrap_or(Value::Null),
    }
}

pub fn message_to_bridge_json(message: &Msg) -> Value {
    let mut value = variant_to_bridge_json(message.as_variant());
    if let (Some(link_stack), Some(object)) = (&message.link_call_stack, value.as_object_mut()) {
        object.insert("_linkSource".into(), serde_json::to_value(link_stack).unwrap_or(Value::Null));
    }
    value
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

#[cfg(test)]
mod tests {
    use super::*;
    use edgelink_core::runtime::model::Variant;

    #[test]
    fn tagged_buffer_crosses_the_python_flow_bridge_both_directions() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let tagged = PyDict::new(py);
            tagged.set_item("$__el_type", "Buffer").unwrap();
            tagged.set_item("data", PyList::new(py, [0u8, 127, 255]).unwrap()).unwrap();
            let json = py_object_to_json_value(tagged.as_any()).unwrap();
            let value: Variant = serde_json::from_value(json).unwrap();
            assert!(matches!(value, Variant::Bytes(ref bytes) if bytes == &[0, 127, 255]));

            let output = variant_to_bridge_json(&Variant::Object(std::collections::BTreeMap::from([(
                "payload".into(),
                Variant::Array(vec![Variant::Bytes(vec![1, 2])]),
            )])));
            assert_eq!(output["payload"][0], serde_json::json!({"$__el_type":"Buffer", "data":[1,2]}));
            let py_output = json_value_to_py_object(py, &output).unwrap();
            let output_json = py_object_to_json_value(py_output.bind(py)).unwrap();
            let round_trip: Variant = serde_json::from_value(output_json).unwrap();
            assert!(
                matches!(round_trip.as_object().unwrap().get("payload"), Some(Variant::Array(items)) if matches!(&items[0], Variant::Bytes(bytes) if bytes == &[1,2]))
            );
        });
    }

    #[test]
    fn tagged_buffer_rejects_out_of_range_and_boolean_bytes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            for data in [vec![256i64], vec![-1]] {
                let tagged = PyDict::new(py);
                tagged.set_item("$__el_type", "Buffer").unwrap();
                tagged.set_item("data", PyList::new(py, data).unwrap()).unwrap();
                assert!(py_object_to_json_value(tagged.as_any()).is_err());
            }
            let tagged = PyDict::new(py);
            tagged.set_item("$__el_type", "Buffer").unwrap();
            tagged.set_item("data", PyList::new(py, [true]).unwrap()).unwrap();
            assert!(py_object_to_json_value(tagged.as_any()).is_err());
        });
    }
}
