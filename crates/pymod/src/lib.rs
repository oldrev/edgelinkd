#![allow(unsafe_op_in_unsafe_fn)] // TODO FIXME

use edgelink_core::runtime::model::{ElementId, Msg};
use pyo3::types::PyModule;
use pyo3::{prelude::*, wrap_pyfunction};
use serde::Deserialize;

use edgelink_core::runtime::engine::Engine;
mod context;
mod json;

#[pymodule]
fn edgelink_pymod(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_sleep, m)?)?;
    m.add_function(wrap_pyfunction!(run_flows_once, m)?)?;
    m.add_function(wrap_pyfunction!(run_flows_for_once, m)?)?;
    m.add_class::<context::PyContextStore>()?;

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

/// Build the engine from the flows and the application config.
fn build_engine_only<'a>(py_json: &'a Bound<'a, PyAny>, app_cfg: &'a Bound<'a, PyAny>) -> PyResult<Engine> {
    let flows_json = json::py_object_to_json_value(py_json)?;
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

    Engine::with_json(&registry, flows_json, app_cfg)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

/// Parse `[[node_id, msg], ...]` - a message injected into a node at the start of the run.
fn parse_injections(msgs_json: &Bound<'_, PyAny>) -> PyResult<Vec<(ElementId, Msg)>> {
    let json_msgs = json::py_object_to_json_value(msgs_json)?;
    Vec::<(ElementId, Msg)>::deserialize(json_msgs)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

/// Parse `[[node_id, msg, delay_ms?], ...]` for the windowed sampler: the optional third
/// element is how long (in milliseconds) the injection waits before it is delivered.
fn parse_scheduled_injections(msgs_json: &Bound<'_, PyAny>) -> PyResult<Vec<(ElementId, Msg, f64)>> {
    let json_msgs = json::py_object_to_json_value(msgs_json)?;
    let Some(entries) = json_msgs.as_array() else {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Injection list expected"));
    };

    let mut injections = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(fields) = entry.as_array() else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("[node_id, msg] expected"));
        };
        if fields.len() < 2 || fields.len() > 3 {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("[node_id, msg, delay_ms?] expected"));
        }
        let node_id = serde_json::from_value::<ElementId>(fields[0].clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let msg = serde_json::from_value::<Msg>(fields[1].clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let delay_ms = match fields.get(2) {
            Some(value) => value
                .as_f64()
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("delay must be a number"))?,
            None => 0.0,
        };
        injections.push((node_id, msg, delay_ms));
    }
    Ok(injections)
}

/// Shared setup for the flow-running entry points: parse the arguments and build the engine.
fn build_engine<'a>(
    py_json: &'a Bound<'a, PyAny>,
    msgs_json: &'a Bound<'a, PyAny>,
    app_cfg: &'a Bound<'a, PyAny>,
) -> PyResult<(Engine, Vec<(ElementId, Msg)>)> {
    let engine = build_engine_only(py_json, app_cfg)?;
    Ok((engine, parse_injections(msgs_json)?))
}

/// Run a flow collection until `expected_msgs` outputs have been produced, or `timeout` expires.
#[pyfunction]
#[pyo3(signature = (_expected_msgs, _timeout, py_json, msgs_json, app_cfg))]
fn run_flows_once<'a>(
    py: Python<'a>,
    _expected_msgs: usize,
    _timeout: f64,
    py_json: &'a Bound<'a, PyAny>,
    msgs_json: &'a Bound<'a, PyAny>,
    app_cfg: &'a Bound<'a, PyAny>,
) -> PyResult<Bound<'a, PyAny>> {
    let (engine, msgs_to_inject) = build_engine(py_json, msgs_json, app_cfg)?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let msgs = engine
            .run_once_with_inject(_expected_msgs, std::time::Duration::from_secs_f64(_timeout), msgs_to_inject)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let result_value = serde_json::to_value(&msgs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Python::with_gil(|py| {
            let pyo = json::json_value_to_py_object(py, &result_value)?;
            Ok(pyo)
        })
    })
}

/// Sample a flow for `_window_seconds` and return whatever it emitted in that window.
///
/// Unlike [`run_flows_once`] this never waits for a message count: it is the time-based
/// sampling that Node-RED's rate-limiting specs use. Every output carries an extra
/// `_arrival_ms` field - its arrival offset relative to the first output - so specs can
/// check the spacing between messages.
///
/// An injection is either `[node_id, msg]` or `[node_id, msg, delay_ms]`; the optional delay
/// is how the drop-rate specs feed a stream of messages instead of one burst.
#[pyfunction]
#[pyo3(signature = (_window_seconds, py_json, msgs_json, app_cfg))]
fn run_flows_for_once<'a>(
    py: Python<'a>,
    _window_seconds: f64,
    py_json: &'a Bound<'a, PyAny>,
    msgs_json: &'a Bound<'a, PyAny>,
    app_cfg: &'a Bound<'a, PyAny>,
) -> PyResult<Bound<'a, PyAny>> {
    let engine = build_engine_only(py_json, app_cfg)?;
    let msgs_to_inject = parse_scheduled_injections(msgs_json)?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let msgs = engine
            .run_window_with_schedule(std::time::Duration::from_secs_f64(_window_seconds), msgs_to_inject)
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
        let result_value = serde_json::Value::Array(out);

        Python::with_gil(|py| {
            let pyo = json::json_value_to_py_object(py, &result_value)?;
            Ok(pyo)
        })
    })
}
