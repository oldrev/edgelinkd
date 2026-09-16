#![allow(unsafe_op_in_unsafe_fn)] // TODO FIXME

use edgelink_core::runtime::model::{ElementId, Msg};
use pyo3::types::PyModule;
use pyo3::{prelude::*, wrap_pyfunction};
use serde::Deserialize;

use edgelink_core::runtime::engine::Engine;
mod json;

#[pymodule]
fn edgelink_pymod(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_sleep, m)?)?;
    m.add_function(wrap_pyfunction!(run_flows_once, m)?)?;

    let stderr = log4rs::append::console::ConsoleAppender::builder()
        .target(log4rs::append::console::Target::Stderr)
        .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new("[{h({l})}]\t{m}{n}")))
        .build();

    let config = log4rs::Config::builder()
        .appender(log4rs::config::Appender::builder().build("stderr", Box::new(stderr)))
        .build(log4rs::config::Root::builder().appender("stderr").build(log::LevelFilter::Warn))
        .unwrap(); // TODO FIXME

    let _ = log4rs::init_config(config).unwrap();

    Ok(())
}

#[pyfunction]
fn rust_sleep(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async {
        eprintln!("Sleeping in Rust!");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (_expected_msgs, _timeout, py_json, msgs_json, app_cfg, windowed=false))]
fn run_flows_once<'a>(
    py: Python<'a>,
    _expected_msgs: usize,
    _timeout: f64,
    py_json: &'a Bound<'a, PyAny>,
    msgs_json: &'a Bound<'a, PyAny>,
    app_cfg: &'a Bound<'a, PyAny>,
    windowed: bool,
) -> PyResult<Bound<'a, PyAny>> {
    let flows_json = json::py_object_to_json_value(py_json)?;
    let msgs_to_inject = {
        let json_msgs = json::py_object_to_json_value(msgs_json)?;
        Vec::<(ElementId, Msg)>::deserialize(json_msgs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
    };
    let app_cfg = {
        if !app_cfg.is_none() {
            let app_cfg_json = json::py_object_to_json_value(app_cfg)?;
            let config = config::Config::try_from(&app_cfg_json)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Some(config)
        } else {
            None
        }
    };

    let registry = edgelink_core::runtime::registry::RegistryBuilder::default()
        .build()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let engine = Engine::with_json(&registry, flows_json, app_cfg)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        // Windowed runs sample the flow for a fixed duration instead of waiting for a count.
        // They also report each output's arrival offset, which the rate-limiting specs need
        // to check the spacing between messages.
        let result_value = if windowed {
            let msgs = engine
                .run_window_with_inject(std::time::Duration::from_secs_f64(_timeout), msgs_to_inject)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            let mut out = Vec::with_capacity(msgs.len());
            for (msg, arrival_ms) in msgs {
                let mut val = serde_json::to_value(&msg)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
                if let serde_json::Value::Object(ref mut map) = val {
                    map.insert("_arrival_ms".to_string(), serde_json::Value::from(arrival_ms));
                }
                out.push(val);
            }
            serde_json::Value::Array(out)
        } else {
            let msgs = engine
                .run_once_with_inject(_expected_msgs, std::time::Duration::from_secs_f64(_timeout), msgs_to_inject)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            serde_json::to_value(&msgs).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        };

        Python::with_gil(|py| {
            let pyo = json::json_value_to_py_object(py, &result_value)?;
            Ok(pyo)
        })
    })
}
