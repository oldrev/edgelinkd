use serde::Deserialize;
use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[cfg(feature = "nodes_xml")]
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
#[cfg(feature = "nodes_xml")]
use quick_xml::{Reader, Writer};
#[cfg(feature = "nodes_xml")]
use std::collections::BTreeMap;
#[cfg(feature = "nodes_xml")]
use std::io::Cursor;

#[derive(Debug)]
#[flow_node("xml", red_name = "XML")]
struct XmlNode {
    base: BaseFlowNodeState,
    config: XmlNodeConfig,
}

impl XmlNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let xml_config = XmlNodeConfig::deserialize(&config.rest)?;

        let node = XmlNode { base: state, config: xml_config };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct XmlNodeConfig {
    /// Property name to operate on (default: "payload")
    #[serde(default = "default_property")]
    property: String,

    /// Attribute key name (default: "$")
    #[serde(default = "default_attr_key")]
    attr: String,

    /// Character key name (default: "_")
    #[serde(default = "default_char_key")]
    chr: String,

    /// Number of outputs (usually 1)
    #[serde(default = "default_outputs")]
    #[allow(dead_code)]
    outputs: usize,
}

fn default_property() -> String {
    "payload".to_string()
}

fn default_attr_key() -> String {
    "$".to_string()
}

fn default_char_key() -> String {
    "_".to_string()
}

fn default_outputs() -> usize {
    1
}

#[cfg(feature = "nodes_xml")]
impl XmlNode {
    async fn process_xml(&self, msg: MsgHandle) -> crate::Result<()> {
        let mut msg_guard = msg.write().await;

        // Get the value from the specified property
        if !msg_guard.contains(&self.config.property) {
            // If property doesn't exist, just pass through
            drop(msg_guard);
            return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
        }

        let property_value = msg_guard.get(&self.config.property).cloned();

        if let Some(value) = property_value {
            // Get options from message if present
            let options = if msg_guard.contains("options") { msg_guard.get("options").cloned() } else { None };

            let result = match &value {
                Variant::String(xml_string) => {
                    // String input: parse XML to object
                    self.parse_xml_to_object(xml_string, options).await
                }
                Variant::Object(_) | Variant::Array(_) => {
                    // Object input: convert to XML string
                    self.convert_object_to_xml(&value, options).await
                }
                _ => {
                    // For other types, issue a warning and pass through
                    log::warn!("XML node expects string or object input, got {value:?}");
                    Ok(value)
                }
            };

            match result {
                Ok(new_value) => {
                    msg_guard[&self.config.property] = new_value;
                }
                Err(e) => {
                    drop(msg_guard);
                    return Err(e);
                }
            }
        }

        drop(msg_guard);
        self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await
    }

    async fn parse_xml_to_object(&self, xml_string: &str, options: Option<Variant>) -> crate::Result<Variant> {
        // Extract options for parsing
        let (attr_key, char_key) = self.extract_parse_options(options);

        // Use quick-xml for parsing with custom attribute and character keys
        let mut reader = Reader::from_str(xml_string);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut stack: Vec<(String, BTreeMap<String, Variant>)> = Vec::new();
        let mut root: Option<BTreeMap<String, Variant>> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    let mut element = BTreeMap::new();

                    // Process attributes
                    let mut attributes = BTreeMap::new();
                    for attr_result in e.attributes() {
                        match attr_result {
                            Ok(attr) => {
                                let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
                                let value = String::from_utf8_lossy(&attr.value).to_string();
                                attributes.insert(key, Variant::String(value));
                            }
                            Err(e) => {
                                return Err(crate::EdgelinkError::InvalidOperation(format!(
                                    "XML attribute parse error: {e}"
                                ))
                                .into());
                            }
                        }
                    }

                    if !attributes.is_empty() {
                        element.insert(attr_key.clone(), Variant::Object(attributes));
                    }

                    stack.push((name, element));
                }
                Ok(Event::End(_)) => {
                    if let Some((element_name, element)) = stack.pop() {
                        if let Some((_, parent)) = stack.last_mut() {
                            // Add to parent
                            if let Some(existing) = parent.get_mut(&element_name) {
                                // Convert to array if multiple elements with same name
                                match existing {
                                    Variant::Array(arr) => {
                                        arr.push(Variant::Object(element));
                                    }
                                    _ => {
                                        let old_value = existing.clone();
                                        *existing = Variant::Array(vec![old_value, Variant::Object(element)]);
                                    }
                                }
                            } else {
                                parent.insert(element_name, Variant::Object(element));
                            }
                        } else {
                            // This is the root element
                            root = Some(BTreeMap::from([(element_name, Variant::Object(element))]));
                        }
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = e
                        .unescape()
                        .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML text unescape error: {e}")))?
                        .trim()
                        .to_string();

                    if !text.is_empty() {
                        if let Some((_, element)) = stack.last_mut() {
                            element.insert(char_key.clone(), Variant::String(text));
                        }
                    }
                }
                Ok(Event::CData(e)) => {
                    let text = String::from_utf8_lossy(&e).to_string();
                    if let Some((_, element)) = stack.last_mut() {
                        element.insert(char_key.clone(), Variant::String(text));
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(crate::EdgelinkError::InvalidOperation(format!("XML parse error: {e}")).into());
                }
                _ => {} // Ignore other events like comments, processing instructions
            }
            buf.clear();
        }

        if let Some(root) = root { Ok(Variant::Object(root)) } else { Ok(Variant::Object(BTreeMap::new())) }
    }

    async fn convert_object_to_xml(&self, value: &Variant, options: Option<Variant>) -> crate::Result<Variant> {
        // Extract options for building
        let _pretty = self.extract_build_options(options);

        let mut writer = Writer::new(Cursor::new(Vec::new()));

        // Write XML declaration
        writer
            .write_event(Event::Decl(quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None)))
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

        match value {
            Variant::Object(obj) => {
                self.write_object_to_xml(&mut writer, obj, &self.config.attr, &self.config.chr)?;
            }
            Variant::Array(arr) => {
                // Wrap array in a root element
                let start = BytesStart::new("root");
                writer
                    .write_event(Event::Start(start.clone()))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                for item in arr {
                    match item {
                        Variant::Object(obj) => {
                            self.write_object_to_xml(&mut writer, obj, &self.config.attr, &self.config.chr)?;
                        }
                        _ => {
                            // Write primitive values as item elements
                            writer
                                .write_event(Event::Start(BytesStart::new("item")))
                                .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                            let text = self.variant_to_xml_text(item);
                            writer
                                .write_event(Event::Text(BytesText::new(&text)))
                                .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                            writer
                                .write_event(Event::End(BytesEnd::new("item")))
                                .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
                        }
                    }
                }

                writer
                    .write_event(Event::End(BytesEnd::new("root")))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
            }
            _ => {
                // For primitive values, wrap in a root element
                writer
                    .write_event(Event::Start(BytesStart::new("root")))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                let text = self.variant_to_xml_text(value);
                writer
                    .write_event(Event::Text(BytesText::new(&text)))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                writer
                    .write_event(Event::End(BytesEnd::new("root")))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
            }
        }

        let result = writer.into_inner().into_inner();
        let xml_string = String::from_utf8(result)
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML encoding error: {e}")))?;

        // If pretty formatting is requested, we would need to format it
        // For now, return as-is since quick-xml doesn't have built-in pretty printing
        Ok(Variant::String(xml_string))
    }

    fn write_object_to_xml<W: std::io::Write>(
        &self,
        writer: &mut Writer<W>,
        obj: &BTreeMap<String, Variant>,
        attr_key: &str,
        char_key: &str,
    ) -> crate::Result<()> {
        for (key, value) in obj {
            let mut start_elem = BytesStart::new(key);

            // Check if this object has attributes or character data
            if let Variant::Object(inner_obj) = value {
                let mut text_content = None;

                // Extract attributes
                if let Some(Variant::Object(attrs)) = inner_obj.get(attr_key) {
                    for (attr_name, attr_value) in attrs {
                        let attr_val = self.variant_to_xml_text(attr_value);
                        start_elem.push_attribute((attr_name.as_bytes(), attr_val.as_bytes()));
                    }
                }

                // Extract text content
                if let Some(text_var) = inner_obj.get(char_key) {
                    text_content = Some(self.variant_to_xml_text(text_var));
                }

                writer
                    .write_event(Event::Start(start_elem.clone()))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                // Write text content if present
                if let Some(text) = text_content {
                    writer
                        .write_event(Event::Text(BytesText::new(&text)))
                        .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
                }

                // Write child elements (skip attribute and character keys)
                for (child_key, child_value) in inner_obj {
                    if child_key != attr_key && child_key != char_key {
                        match child_value {
                            Variant::Array(arr) => {
                                // Multiple elements with same name
                                for item in arr {
                                    if let Variant::Object(_item_obj) = item {
                                        let child_obj = BTreeMap::from([(child_key.clone(), item.clone())]);
                                        self.write_object_to_xml(writer, &child_obj, attr_key, char_key)?;
                                    }
                                }
                            }
                            Variant::Object(_) => {
                                let child_obj = BTreeMap::from([(child_key.clone(), child_value.clone())]);
                                self.write_object_to_xml(writer, &child_obj, attr_key, char_key)?;
                            }
                            _ => {
                                // Write as text element
                                writer.write_event(Event::Start(BytesStart::new(child_key))).map_err(|e| {
                                    crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}"))
                                })?;

                                let text = self.variant_to_xml_text(child_value);
                                writer.write_event(Event::Text(BytesText::new(&text))).map_err(|e| {
                                    crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}"))
                                })?;

                                writer.write_event(Event::End(BytesEnd::new(child_key))).map_err(|e| {
                                    crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}"))
                                })?;
                            }
                        }
                    }
                }

                writer
                    .write_event(Event::End(BytesEnd::new(key)))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
            } else {
                // Simple value
                writer
                    .write_event(Event::Start(start_elem.clone()))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                let text = self.variant_to_xml_text(value);
                writer
                    .write_event(Event::Text(BytesText::new(&text)))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

                writer
                    .write_event(Event::End(BytesEnd::new(key)))
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
            }
        }

        Ok(())
    }

    fn variant_to_xml_text(&self, variant: &Variant) -> String {
        match variant {
            Variant::String(s) => s.clone(),
            Variant::Number(n) => n.to_string(),
            Variant::Bool(b) => b.to_string(),
            Variant::Null => String::new(),
            _ => format!("{variant:?}"), // Fallback for complex types
        }
    }

    fn extract_parse_options(&self, options: Option<Variant>) -> (String, String) {
        let mut attr_key = self.config.attr.clone();
        let mut char_key = self.config.chr.clone();

        if let Some(Variant::Object(opts)) = options {
            if let Some(Variant::String(ak)) = opts.get("attrkey") {
                attr_key = ak.clone();
            }
            if let Some(Variant::String(ck)) = opts.get("charkey") {
                char_key = ck.clone();
            }
        }

        (attr_key, char_key)
    }

    fn extract_build_options(&self, options: Option<Variant>) -> bool {
        if let Some(Variant::Object(opts)) = options {
            if let Some(Variant::Object(render_opts)) = opts.get("renderOpts") {
                if let Some(Variant::Bool(pretty)) = render_opts.get("pretty") {
                    return *pretty;
                }
            }
        }
        false
    }
}

#[cfg(not(feature = "nodes_xml"))]
impl XmlNode {
    async fn process_xml(&self, _msg: MsgHandle) -> crate::Result<()> {
        log::error!("XML node is not available. Please enable the 'nodes_xml' feature.");
        Err(crate::EdgelinkError::InvalidOperation("XML node requires 'nodes_xml' feature to be enabled".to_string())
            .into())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for XmlNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();

            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move { node.process_xml(msg).await }).await;
        }
    }
}
