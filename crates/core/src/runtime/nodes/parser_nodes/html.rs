use std::collections::BTreeMap;
use std::sync::Arc;

use scraper::{ElementRef, Html, Selector};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::runtime::flow::Flow;
use crate::runtime::model::Variant;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum HtmlReturn {
    #[default]
    Html,
    Text,
    Attr,
    Compl,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum HtmlOutput {
    #[default]
    Single,
    Multi,
}

#[derive(Debug, Deserialize)]
struct HtmlConfig {
    #[serde(default = "default_property")]
    property: String,
    #[serde(default)]
    outproperty: Option<String>,
    #[serde(default)]
    tag: String,
    #[serde(default)]
    ret: HtmlReturn,
    #[serde(default)]
    r#as: HtmlOutput,
    #[serde(default = "default_chr")]
    chr: String,
}

fn default_property() -> String {
    "payload".to_string()
}
fn default_chr() -> String {
    "_".to_string()
}

#[derive(Debug)]
#[flow_node("html", red_name = "HTML")]
struct HtmlNode {
    base: BaseFlowNodeState,
    config: HtmlConfig,
}

impl HtmlNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        Ok(Box::new(Self { base: state, config: HtmlConfig::deserialize(&config.rest)? }))
    }

    fn value(element: ElementRef<'_>, ret: HtmlReturn, chr: &str) -> Variant {
        match ret {
            HtmlReturn::Html => Variant::String(element.inner_html().trim().to_string()),
            HtmlReturn::Text => Variant::String(element.text().collect::<String>()),
            HtmlReturn::Attr => Variant::Object(
                element.value().attrs().map(|(k, v)| (k.to_string(), Variant::String(v.to_string()))).collect(),
            ),
            HtmlReturn::Compl => {
                let mut obj: BTreeMap<String, Variant> =
                    element.value().attrs().map(|(k, v)| (k.to_string(), Variant::String(v.to_string()))).collect();
                obj.insert(chr.to_string(), Variant::String(element.inner_html().trim().to_string()));
                Variant::Object(obj)
            }
        }
    }

    async fn process(&self, msg: MsgHandle) -> crate::Result<()> {
        let mut guard = msg.write().await;
        let Some(input) = guard.get_nav_stripped(&self.config.property).cloned() else {
            drop(guard);
            return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
        };
        let Some(source) = input.as_str() else {
            return Err(crate::EdgelinkError::BadArgument("html input must be a string").into());
        };
        let selector_text = guard
            .get("select")
            .and_then(Variant::as_str)
            .filter(|_| self.config.tag.is_empty())
            .unwrap_or(&self.config.tag);
        let selector =
            Selector::parse(selector_text).map_err(|_| crate::EdgelinkError::BadArgument("invalid CSS selector"))?;
        let values: Vec<Variant> = {
            let document = Html::parse_document(source);
            document.select(&selector).map(|el| Self::value(el, self.config.ret, &self.config.chr)).collect()
        };
        let outproperty = self.config.outproperty.as_deref().unwrap_or(&self.config.property);
        if matches!(self.config.r#as, HtmlOutput::Single) {
            guard.set_nav_stripped(outproperty, Variant::Array(values), true)?;
            drop(guard);
            self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await
        } else {
            let count = values.len();
            let id = guard.id().unwrap_or_else(crate::runtime::model::Msg::generate_id);
            let mut outputs = Vec::with_capacity(count);
            for (index, value) in values.into_iter().enumerate() {
                let out = if index == 0 { msg.clone() } else { msg.deep_clone(true).await };
                let mut out_guard = out.write().await;
                out_guard.set_nav_stripped(outproperty, value, true)?;
                let mut parts = BTreeMap::new();
                parts.insert("id".to_string(), Variant::String(id.to_string()));
                parts.insert("index".to_string(), Variant::Number(serde_json::Number::from(index)));
                parts.insert("count".to_string(), Variant::Number(serde_json::Number::from(count)));
                parts.insert("type".to_string(), Variant::String("string".to_string()));
                parts.insert("ch".to_string(), Variant::String(String::new()));
                out_guard.set("parts".to_string(), Variant::Object(parts));
                drop(out_guard);
                outputs.push(Envelope { port: 0, msg: out });
            }
            drop(guard);
            self.fan_out_many(outputs.into_iter().collect(), CancellationToken::new()).await
        }
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for HtmlNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();
            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move { node.process(msg).await }).await;
        }
    }
}
