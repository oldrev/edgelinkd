use serde_json::Value;

pub static EMPTY_ARRAY: Vec<serde_json::Value> = Vec::new();

pub fn value_equals_str(jv: &Value, target_string: &str) -> bool {
    match jv.as_str() {
        Some(s) => s == target_string,
        _ => false,
    }
}

pub fn option_value_equals_str(jv: &Option<&Value>, target_string: &str) -> bool {
    match jv {
        Some(s) => value_equals_str(s, target_string),
        _ => false,
    }
}

pub trait JsonValueExt {
    fn get_str(&self, key: &str) -> Option<&str>;
    fn get_str_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str;
}

impl JsonValueExt for Value {
    fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|x| x.as_str())
    }

    fn get_str_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key).and_then(|x| x.as_str()).unwrap_or(default)
    }
}

impl JsonValueExt for serde_json::Map<String, Value> {
    fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|x| x.as_str())
    }

    fn get_str_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key).and_then(|x| x.as_str()).unwrap_or(default)
    }
}
