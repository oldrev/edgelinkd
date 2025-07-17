use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::{Reader, Writer};
use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone)]
pub struct Xml2jsOptions {
    pub explicit_array: bool,
    pub explicit_root: bool,
    pub merge_attrs: bool,
    pub explicit_charkey: bool,
    pub charkey: String,
    pub attrkey: String,
}

impl Default for Xml2jsOptions {
    fn default() -> Self {
        Xml2jsOptions {
            explicit_array: true,
            explicit_root: true,
            merge_attrs: true,
            explicit_charkey: false,
            charkey: "_".to_string(),
            attrkey: "$".to_string(),
        }
    }
}

#[derive(Debug)]
#[flow_node("xml", red_name = "XML")]
struct XmlNode {
    base: BaseFlowNodeState,
    config: XmlNodeConfig,
    default_options: Xml2jsOptions,
}

impl XmlNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let xml_config = XmlNodeConfig::deserialize(&config.rest)?;
        let node = XmlNode { base: state, config: xml_config, default_options: Xml2jsOptions::default() };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct XmlNodeConfig {
    /// Property name to operate on (default: "payload")
    #[serde(default = "default_property")]
    property: String,

    /// Attribute key name
    #[serde(default)]
    attr: Option<String>,

    /// Character key name
    #[serde(default)]
    chr: Option<String>,

    /// Number of outputs (usually 1)
    #[serde(default = "default_outputs")]
    #[allow(dead_code)]
    outputs: usize,
}

fn default_property() -> String {
    "payload".to_string()
}

fn default_outputs() -> usize {
    1
}

#[cfg(feature = "nodes_xml")]
impl XmlNode {
    async fn process_xml(&self, msg: MsgHandle) -> crate::Result<()> {
        let mut msg_guard = msg.write().await;

        if !msg_guard.contains(&self.config.property) {
            drop(msg_guard);
            return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
        }

        let property_value = msg_guard.get(&self.config.property);

        if let Some(value) = property_value {
            let result = match value {
                Variant::String(xml_string) => {
                    let mut options = if let Some(options_var) = msg_guard.get("options") {
                        self.extract_known_options_properties(options_var)
                    } else {
                        self.default_options.clone()
                    };
                    if let Some(attr) = &self.config.attr {
                        options.attrkey = attr.clone();
                    }
                    if let Some(chr) = &self.config.chr {
                        options.charkey = chr.clone();
                    }
                    self.parse_xml_to_object(xml_string, &options)
                }
                Variant::Object(_) => {
                    let mut options = if let Some(options_var) = msg_guard.get("options") {
                        self.extract_known_options_properties(options_var)
                    } else {
                        self.default_options.clone()
                    };
                    if let Some(attr) = &self.config.attr {
                        options.attrkey = attr.clone();
                    }
                    if let Some(chr) = &self.config.chr {
                        options.charkey = chr.clone();
                    }
                    self.convert_object_to_xml(&value, &options)
                }
                _ => {
                    log::warn!("XML node expects string or object input, got {value:?}");
                    Ok(value.clone())
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

    fn parse_xml_to_object(&self, xml_string: &str, options: &Xml2jsOptions) -> crate::Result<Variant> {
        xml_to_variant(xml_string, options)
    }

    fn convert_object_to_xml(&self, value: &Variant, options: &Xml2jsOptions) -> crate::Result<Variant> {
        // Extract options for building
        //let _pretty = self.extract_build_options(options);
        let _pretty = true;

        let mut writer = Writer::new(Cursor::new(Vec::new()));

        // Write XML declaration
        writer
            .write_event(Event::Decl(quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None)))
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;

        match value {
            Variant::Object(obj) => {
                self.write_object_to_xml(&mut writer, obj, options)?;
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
                            self.write_object_to_xml(&mut writer, obj, options)?;
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
        options: &Xml2jsOptions,
    ) -> crate::Result<()> {
        for (key, value) in obj {
            match value {
                Variant::Array(arr) => {
                    for item in arr {
                        let mut single = BTreeMap::new();
                        single.insert(key.clone(), item.clone());
                        self.write_object_to_xml(writer, &single, options)?;
                    }
                }
                Variant::Object(inner_obj) => {
                    let mut start_elem = BytesStart::new(key);
                    if let Some(Variant::Object(attrs)) = inner_obj.get(&options.attrkey) {
                        for (attr_name, attr_value) in attrs {
                            let attr_val = self.variant_to_xml_text(attr_value);
                            start_elem.push_attribute((attr_name.as_bytes(), attr_val.as_bytes()));
                        }
                    }
                    writer
                        .write_event(Event::Start(start_elem.clone()))
                        .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
                    if let Some(text_var) = inner_obj.get(&options.charkey) {
                        let text = self.variant_to_xml_text(text_var);
                        writer
                            .write_event(Event::Text(BytesText::new(&text)))
                            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
                    }
                    for (child_key, child_value) in inner_obj {
                        if *child_key != options.attrkey && *child_key != options.charkey {
                            let mut single = BTreeMap::new();
                            single.insert(child_key.clone(), child_value.clone());
                            self.write_object_to_xml(writer, &single, options)?;
                        }
                    }
                    writer
                        .write_event(Event::End(BytesEnd::new(key)))
                        .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("XML write error: {e}")))?;
                }
                _ => {
                    writer
                        .write_event(Event::Start(BytesStart::new(key)))
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

    fn extract_known_options_properties(&self, options_var: &Variant) -> Xml2jsOptions {
        let mut opts = Xml2jsOptions::default();
        if let Some(opts_obj) = options_var.as_object() {
            if let Some(Variant::Bool(ea)) = opts_obj.get("explicit_array") {
                opts.explicit_array = *ea;
            }
            if let Some(Variant::Bool(er)) = opts_obj.get("explicit_root") {
                opts.explicit_root = *er;
            }
            if let Some(Variant::Bool(ma)) = opts_obj.get("merge_attrs") {
                opts.merge_attrs = *ma;
            }
            if let Some(Variant::Bool(ec)) = opts_obj.get("explicit_charkey") {
                opts.explicit_charkey = *ec;
            }
            if let Some(Variant::String(ak)) = opts_obj.get("attrkey") {
                opts.attrkey = ak.clone();
            }
            if let Some(Variant::String(ck)) = opts_obj.get("charkey") {
                opts.charkey = ck.clone();
            }
        }
        opts
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

fn xml_to_variant(xml_string: &str, options: &Xml2jsOptions) -> crate::Result<Variant> {
    // Extract options for parsing
    //let (attr_key, char_key) = self.extract_parse_options(options);
    let mut reader = Reader::from_reader(xml_string.as_bytes());
    // reader.trim_text(true);

    let mut text_buf = String::new();
    let mut stack: Vec<(String, VariantObjectMap)> = Vec::new();
    let mut root_tag: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if root_tag.is_none() {
                    root_tag = Some(tag.clone());
                }

                let mut map = VariantObjectMap::new();
                let attrs = attributes_to_map(e, options)?;

                if options.merge_attrs {
                    for (k, v) in attrs {
                        map.insert(k, v);
                    }
                } else if !attrs.is_empty() {
                    map.insert(options.attrkey.clone(), Variant::Object(attrs));
                }

                stack.push((tag, map));
                text_buf.clear();
            }

            Ok(Event::Text(e)) => {
                text_buf.push_str(&e.unescape().unwrap_or_default());
            }

            Ok(Event::End(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let (open_tag, mut obj) = stack.pop().unwrap();

                if open_tag != tag {
                    return Err(EdgelinkError::InvalidOperation(format!(
                        "Mismatched tag: expected </{}> got </{}>",
                        open_tag, tag
                    ))
                    .into());
                }

                if !text_buf.trim().is_empty() {
                    let text = Variant::String(text_buf.trim().to_string());

                    if options.explicit_charkey {
                        obj.insert(options.charkey.clone(), text);
                    } else {
                        if obj.is_empty() {
                            insert_variant_value(&mut stack, &tag, text, options);
                            text_buf.clear();
                            continue;
                        } else {
                            obj.insert(options.charkey.clone(), text);
                        }
                    }
                }

                let value = Variant::Object(obj);
                insert_variant_value(&mut stack, &tag, value, options);
                text_buf.clear();
            }

            Ok(Event::Eof) => break,
            Err(e) => return Err(EdgelinkError::InvalidOperation(format!("XML parse error: {}", e)).into()),
            _ => {}
        }
    }

    if let Some((_last_tag, last_obj)) = stack.pop() {
        let content = Variant::Object(last_obj);
        if options.explicit_root {
            let mut root = VariantObjectMap::new();
            root.insert(root_tag.unwrap_or_else(|| "root".to_string()), content);
            Ok(Variant::Object(root))
        } else {
            Ok(content)
        }
    } else {
        Err(EdgelinkError::InvalidOperation("No root element".to_string()).into())
    }
}

fn attributes_to_map(e: &BytesStart, _options: &Xml2jsOptions) -> crate::Result<VariantObjectMap> {
    let mut attrs = VariantObjectMap::new();
    for attr in e.attributes().flatten() {
        let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
        let val = attr.unescape_value().unwrap_or_default().to_string();
        attrs.insert(key, Variant::String(val));
    }
    Ok(attrs)
}

fn insert_variant_value(
    stack: &mut Vec<(String, VariantObjectMap)>,
    tag: &str,
    value: Variant,
    options: &Xml2jsOptions,
) {
    if let Some((_parent_tag, parent)) = stack.last_mut() {
        match parent.get_mut(tag) {
            Some(existing) => match existing {
                Variant::Array(arr) => arr.push(value),
                old => {
                    let replaced = std::mem::replace(old, Variant::Array(vec![]));
                    if let Variant::Array(arr) = old {
                        *arr = vec![replaced, value];
                    }
                }
            },
            None => {
                if options.explicit_array {
                    parent.insert(tag.to_string(), Variant::Array(vec![value]));
                } else {
                    parent.insert(tag.to_string(), value);
                }
            }
        }
    } else {
        match value {
            Variant::Object(map) => {
                stack.push((tag.to_string(), map));
            }
            _ => {
                let mut obj = VariantObjectMap::new();
                obj.insert(tag.to_string(), value);
                stack.push((tag.to_string(), obj));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_employees_structure() {
        let xml = r#"<employees><firstName>John</firstName><lastName>Smith</lastName></employees>"#;
        // should be: {'employees': {'firstName': ['John'], 'lastName': ['Smith']}}
        let options = Xml2jsOptions::default();

        let result = xml_to_variant(xml, &options).unwrap();
        let root = result.as_object().unwrap();
        let employees = root.get("employees").unwrap().as_object().unwrap();
        assert_eq!(employees.get("firstName").unwrap().as_array().unwrap()[0].as_str().unwrap(), "John");
        assert_eq!(employees.get("lastName").unwrap().as_array().unwrap()[0].as_str().unwrap(), "Smith");
    }
}
