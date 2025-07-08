use core::f64;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use crate::text::parsing;
use edgelink_macro::*;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Deserialize)]
enum RbeFunc {
    #[default]
    #[serde(rename = "rbe")]
    Rbe,

    #[serde(rename = "rbei")]
    Rbei,

    #[serde(rename = "narrowband")]
    Narrowband,

    #[serde(rename = "narrowbandEq")]
    NarrowbandEq,

    #[serde(rename = "deadband")]
    Deadband,

    #[serde(rename = "deadbandEq")]
    DeadbandEq,
}

impl RbeFunc {
    fn is_rbe(&self) -> bool {
        matches!(self, RbeFunc::Rbe | RbeFunc::Rbei)
    }

    fn is_narrowband(&self) -> bool {
        matches!(self, RbeFunc::Narrowband | RbeFunc::NarrowbandEq)
    }

    fn is_deadband(&self) -> bool {
        matches!(self, RbeFunc::Deadband | RbeFunc::DeadbandEq)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Deserialize)]
enum Inout {
    #[serde(rename = "in")]
    In,

    #[default]
    #[serde(rename = "out")]
    Out,
}

#[derive(Debug, Clone, Deserialize)]
struct RbeNodeConfig {
    #[serde(default)]
    func: RbeFunc,

    #[serde(deserialize_with = "deser_f64_percent_or_0")]
    gap: f64,

    #[serde(skip, default)]
    is_percent: bool,

    #[serde(default, rename = "start", deserialize_with = "json::deser::str_to_option_f64")]
    start_value: Option<f64>,

    #[serde(rename = "septopics", default = "rbe_setopics_default")]
    sep_topics: bool,

    #[serde(rename = "property", default = "rbe_property_default")]
    property: String,

    #[serde(rename = "topi", default = "rbe_topi_default")]
    topic: String,

    #[serde(default)]
    inout: Inout,
}

fn rbe_setopics_default() -> bool {
    true
}

fn rbe_property_default() -> String {
    "payload".to_owned()
}

fn rbe_topi_default() -> String {
    "topic".to_owned()
}

fn deser_f64_percent_or_0<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value: serde_json::Value = Deserialize::deserialize(deserializer)?;

    match value {
        // If it's already a float, return it directly
        serde_json::Value::Number(num) => num.as_f64().ok_or_else(|| serde::de::Error::custom("Invalid f64")),

        // If it's a string, handle different cases
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(0.0)
            } else if let Some(stripped) = trimmed.strip_suffix('%') {
                // Remove the '%' and parse the rest as a number, divide by 100
                let percentage = stripped;
                percentage
                    .parse::<f64>()
                    .map(|n| n / 100.0)
                    .map_err(|_| serde::de::Error::custom("Invalid percentage format"))
            } else {
                // Try to parse the string directly as an f64
                f64::from_str(trimmed).map_err(|_| serde::de::Error::custom("Invalid f64 format"))
            }
        }

        // Any other type is invalid for this deserialization
        _ => Err(serde::de::Error::custom("Invalid type for f64")),
    }
}

#[derive(Debug)]
struct RbeNodeState {
    current_gap: f64,
    prev: HashMap<String, Variant>,
}

impl Default for RbeNodeState {
    fn default() -> Self {
        Self { current_gap: 0.0, prev: HashMap::new() }
    }
}

#[derive(Debug)]
#[flow_node("rbe", red_name = "rbe")]
struct RbeNode {
    base: BaseFlowNodeState,
    config: RbeNodeConfig,
    state: Mutex<RbeNodeState>,
}

impl RbeNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut rbe_config = RbeNodeConfig::deserialize(&config.rest)?;
        rbe_config.is_percent =
            config.rest.get("gap").and_then(|x| x.as_str()).is_some_and(|x| x.trim().ends_with('%'));

        let node = RbeNode { base: base_node, config: rbe_config, state: Mutex::new(RbeNodeState::default()) };

        Ok(Box::new(node))
    }

    fn do_filter(&self, msg: &mut Msg, state: &mut RbeNodeState) -> bool {
        let topic = msg.get_nav_stripped(&self.config.topic);
        let value = msg.get_nav_stripped(&self.config.property);

        // Handle reset logic
        match (msg.get("reset"), self.config.sep_topics, topic) {
            (Some(_), true, Some(Variant::String(topic))) if !topic.is_empty() => {
                state.prev.remove(topic);
            }
            (Some(_), _, _) => state.prev.clear(),
            (_, _, _) => {}
        }

        // Process value if available
        if let Some(value) = value {
            let t = match (self.config.sep_topics, topic) {
                (true, Some(Variant::String(topic))) => topic.as_str(),
                (_, _) => "_no_topic",
            };
            if self.config.func.is_rbe() {
                let prev_value = state.prev.get_mut(t);
                let do_send = self.config.func != RbeFunc::Rbei || prev_value.is_some();
                // Compare and clone object/value if changed
                return if let Some(pv) = prev_value {
                    if *pv != *value {
                        *pv = value.clone();
                        do_send
                    } else {
                        false
                    }
                } else {
                    state.prev.insert(t.to_string(), value.clone());
                    do_send
                };
            } else {
                let num_value = match value {
                    Variant::Number(v) => v.as_f64().unwrap(), // FIXME
                    Variant::String(s) => parsing::parse_float_lossy::<f64>(s).unwrap_or(f64::NAN),
                    _ => f64::NAN,
                };

                if num_value.is_nan() {
                    log::warn!("The value '{value:?}' is not a number");
                    return false;
                }

                // Read node.previous[t] value
                let mut prev_value = state.prev.get(t).and_then(|x| x.as_f64());

                // Handle the initial value of previous_value
                if prev_value.is_none() && self.config.func.is_narrowband() {
                    prev_value = if let Some(sv) = self.config.start_value { Some(sv) } else { Some(num_value) };
                }

                // Calculate node.gap
                state.current_gap = if self.config.is_percent {
                    (prev_value.unwrap_or(0.0) * self.config.gap).abs()
                } else {
                    self.config.gap
                };

                // Ensure previous_value is set for narrowbandEq
                if prev_value.is_none() && self.config.func == RbeFunc::NarrowbandEq {
                    prev_value = Some(num_value);
                }

                // Handle the case where previous_value is undefined
                if prev_value.is_none() {
                    prev_value = Some(num_value - state.current_gap - 1.0);
                }

                let mut do_send = false;
                // Process the difference and send messages accordingly
                let diff = (num_value - prev_value.unwrap_or(0.0)).abs();

                if (diff == state.current_gap
                    && (self.config.func == RbeFunc::DeadbandEq || self.config.func == RbeFunc::Narrowband))
                    || (diff > state.current_gap && self.config.func.is_deadband())
                    || (diff < state.current_gap && self.config.func.is_narrowband())
                {
                    if self.config.inout == Inout::Out {
                        prev_value = Some(num_value);
                    }
                    do_send = true;
                }

                // Finally, update node.previous[t]
                if self.config.inout == Inout::In {
                    prev_value = Some(num_value);
                }

                // Store the updated previous_value back to node.previous[t]
                if let Some(pv) = prev_value {
                    if let Some(prev) = state.prev.get_mut(t) {
                        *prev = Variant::from(pv);
                    } else {
                        state.prev.insert(t.to_string(), Variant::from(pv));
                    }
                }

                return do_send;
            }
        }

        false
    }
}

#[async_trait]
impl FlowNodeBehavior for RbeNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let can_send = {
                    let mut msg_guard = msg.write().await;
                    let mut state_guard = node.state.lock().await;
                    node.do_filter(&mut msg_guard, &mut state_guard)
                };
                if can_send {
                    node.fan_out_one(Envelope { port: 0, msg }, cancel.child_token()).await?;
                }
                Ok(())
            })
            .await;
        }

        log::debug!("DebugNode process() task has been terminated.");
    }
}
