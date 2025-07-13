use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock, Weak},
};

use dashmap::DashMap;
use itertools::Itertools;
use nom::{self, Parser};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use utils::topo::TopologicalSorter;

use crate::runtime::model::{RedPropertyType, Variant};
use crate::*;

#[derive(Debug, Clone)]
pub struct RedEnvs {
    inner: Arc<RedEnvStore>,
}

#[derive(Debug, Clone)]
pub struct WeakRedEnvs {
    inner: Weak<RedEnvStore>,
}

impl WeakRedEnvs {
    pub fn upgrade(&self) -> Option<RedEnvs> {
        Weak::upgrade(&self.inner).map(|x| RedEnvs { inner: x })
    }
}

#[derive(Debug)]
struct RedEnvStore {
    parent: RwLock<Option<WeakRedEnvs>>,
    envs: DashMap<String, Variant>,
}

impl RedEnvs {
    pub fn downgrade(&self) -> WeakRedEnvs {
        WeakRedEnvs { inner: Arc::downgrade(&self.inner) }
    }

    pub fn evalute_env(&self, env_expr: &str) -> Option<Variant> {
        self.get_normalized(env_expr)
    }

    fn get_raw_env(&self, key: &str) -> Option<Variant> {
        if let Some(value) = self.inner.envs.get(key) {
            Some(value.clone())
        } else {
            let parent = self.inner.parent.read().ok()?;
            parent.as_ref().and_then(|p| p.upgrade()).and_then(|p| p.get_raw_env(key))
        }
    }

    fn get_normalized(&self, env_expr: &str) -> Option<Variant> {
        let trimmed = env_expr.trim();
        if trimmed.starts_with("${") && env_expr.ends_with("}") {
            // ${ENV_VAR}
            let to_match = &trimmed[2..(env_expr.len() - 1)];
            self.get_raw_env(to_match)
        } else if !trimmed.contains("${") {
            // ENV_VAR
            self.get_raw_env(trimmed)
        } else {
            // FOO${ENV_VAR}BAR
            Some(Variant::String(replace_vars(trimmed, |env_name| match self.get_raw_env(env_name) {
                Some(v) => v.to_string().unwrap(), // FIXME
                _ => "".to_owned(),
            })))
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct EnvEntry {
    pub name: String,

    pub value: String,

    #[serde(alias = "type")]
    pub type_: RedPropertyType,
}

#[derive(Debug, Default, Clone)]
pub struct RedEnvStoreBuilder {
    parent: Option<WeakRedEnvs>,
    envs: HashMap<String, Variant>,
}

impl RedEnvStoreBuilder {
    pub fn with_parent(mut self, parent: &RedEnvs) -> Self {
        self.parent = Some(parent.downgrade());
        self
    }

    pub fn load_json(mut self, jv: &JsonValue) -> Self {
        if let Ok(mut entries) = Vec::<EnvEntry>::deserialize(jv) {
            // Remove duplicated by name, only keep the last one
            entries = {
                let mut seen = HashSet::new();
                entries
                    .into_iter()
                    .rev()
                    .unique_by(|e| e.name.clone())
                    .filter(|e| seen.insert(e.name.clone()))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect()
            };

            let mut topo = TopologicalSorter::new();
            for entry in entries.iter() {
                topo.add_vertex(entry.name.as_str());
                if entry.type_ == RedPropertyType::Env {
                    topo.add_dep(entry.name.as_str(), entry.value.as_str());
                }
            }
            let sorted_keys = topo.dependency_sort();

            for key in sorted_keys.iter() {
                if let Some(e) = entries.iter().find(|x| &x.name == key) {
                    if let Ok(var) = self.evaluate(&e.value, e.type_) {
                        self.envs.insert(e.name.clone(), var);
                    } else {
                        log::warn!("Failed to evaluate environment variable property: {e:?}");
                    }
                }
            }
        } else {
            log::warn!("Failed to parse environment variables: \n{}", serde_json::to_string_pretty(&jv).unwrap());
        }
        self
    }

    pub fn with_process_env(mut self) -> Self {
        for (k, v) in std::env::vars() {
            self.envs.insert(k, Variant::String(v));
        }
        self
    }

    pub fn extends(mut self, other_iter: impl IntoIterator<Item = (String, Variant)>) -> Self {
        for (k, v) in other_iter {
            self.envs.entry(k).or_insert(v);
        }
        self
    }

    pub fn extends_with(mut self, other: &RedEnvs) -> Self {
        for it in other.inner.envs.iter() {
            if !self.envs.contains_key(it.key()) {
                self.envs.insert(it.key().clone(), it.value().clone());
            }
        }
        self
    }

    pub fn update_with(mut self, other: &RedEnvs) -> Self {
        for guard in other.inner.envs.iter() {
            self.envs.insert(guard.key().clone(), guard.value().clone());
        }
        self
    }

    pub fn build(self) -> RedEnvs {
        let mut inner = RedEnvStore { parent: RwLock::new(self.parent), envs: DashMap::with_capacity(self.envs.len()) };
        inner.envs.extend(self.envs);

        RedEnvs { inner: Arc::new(inner) }
    }

    fn evaluate(&self, value: &str, type_: RedPropertyType) -> crate::Result<Variant> {
        match type_ {
            RedPropertyType::Str => Ok(Variant::String(value.into())),

            RedPropertyType::Num | RedPropertyType::Json => {
                let jv: serde_json::Value = serde_json::from_str(value)?;
                Ok(Variant::deserialize(jv)?)
            }

            RedPropertyType::Bool => Ok(Variant::Bool(value.trim_ascii().parse::<bool>()?)),

            RedPropertyType::Bin => {
                let jv: serde_json::Value = serde_json::from_str(value)?;
                let arr = Variant::deserialize(&jv)?;
                let bytes = arr
                    .to_bytes()
                    .ok_or(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Expected an array of bytes, got: {value:?}"))?;
                Ok(Variant::Bytes(bytes))
            }

            RedPropertyType::Jsonata => todo!(),

            RedPropertyType::Env => match self.normalized_and_get_existed(value) {
                Some(ev) => Ok(ev),
                _ => Err(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Cannot found the environment variable: '{value}'")),
            },

            _ => Err(EdgelinkError::BadArgument("type_"))
                .with_context(|| format!("Unsupported environment varibale type: '{value}'")),
        }
    }

    fn get_existed(&self, env: &str) -> Option<Variant> {
        if let Some(value) = self.envs.get(env) {
            Some(value.clone())
        } else {
            self.parent.as_ref().and_then(|p| p.upgrade()).and_then(|p| p.evalute_env(env))
        }
    }

    fn normalized_and_get_existed(&self, value: &str) -> Option<Variant> {
        let trimmed = value.trim();
        if trimmed.starts_with("${") && value.ends_with("}") {
            // ${ENV_VAR}
            let to_match = &trimmed[2..(value.len() - 1)];
            self.get_existed(to_match)
        } else if !trimmed.contains("${") {
            // ENV_VAR
            self.get_existed(trimmed)
        } else {
            // FOO${ENV_VAR}BAR
            Some(Variant::String(replace_vars(trimmed, |env_name| {
                match self.get_existed(env_name) {
                    Some(v) => v.to_string().unwrap(), // FIXME
                    _ => "".to_owned(),
                }
            })))
        }
    }
}

pub fn replace_vars<'a, F, R>(input: &'a str, converter: F) -> String
where
    F: Fn(&'a str) -> R,
    R: AsRef<str>,
{
    fn variable_name(input: &str) -> nom::IResult<&str, &str> {
        nom::sequence::delimited(
            nom::bytes::complete::tag("${"), // Starts with "${"
            nom::sequence::preceded(
                nom::character::complete::space0,
                nom::bytes::complete::take_while(|c: char| c.is_alphanumeric() || c == '_'),
            ),
            nom::sequence::preceded(nom::character::complete::space0, nom::bytes::complete::tag("}")), // Ends with "}"
        )
        .parse(input)
    }

    let mut output = input.to_string();
    let mut remaining_input = input;

    // Continue the parsing until it end
    while let Ok((remaining, var)) = variable_name(remaining_input) {
        let replacement = converter(var);
        output = output.replace(&format!("${{{}}}", var.trim()), replacement.as_ref());
        remaining_input = remaining;
    }

    output
}

pub fn parse_complex_env(expr: &str) -> Option<&str> {
    match parse_complex_env_internal(expr) {
        Ok((_, x)) => Some(x),
        Err(_) => None,
    }
}

fn parse_complex_env_internal(input: &str) -> nom::IResult<&str, &str> {
    nom::sequence::delimited(
        nom::bytes::complete::tag("${"),
        nom::sequence::delimited(
            nom::character::complete::multispace0,
            nom::bytes::complete::take_while(|c: char| c.is_alphanumeric() || c == '_'),
            nom::character::complete::multispace0,
        ),
        nom::bytes::complete::tag("}"),
    )
    .parse(input)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RedEnvStoreBuilder;
    use crate::runtime::model::*;

    #[test]
    fn test_env_store_builder() {
        let json = json!([
            {
                "name": "FOO",
                "value": "foofoo",
                "type": "str"
            },
            {
                "name": "AGE",
                "value": "41",
                "type": "num"
            },
        ]);
        let global =
            RedEnvStoreBuilder::default().load_json(&json).extends([("FILE_SIZE".into(), Variant::from(123))]).build();
        assert_eq!(global.evalute_env("FOO").unwrap().as_str().unwrap(), "foofoo");
        assert_eq!(global.evalute_env("AGE").unwrap().as_i64().unwrap(), 41);

        let json = json!([
            {
                "name": "BAR",
                "value": "barbar",
                "type": "str"
            },
        ]);
        let flow = RedEnvStoreBuilder::default().with_parent(&global).load_json(&json).build();

        let json = json!([
            {
                "name": "MY_FOO",
                "value": "aaa",
                "type": "str"
            },
            {
                "name": "GLOBAL_FOO",
                "value": "FOO",
                "type": "env"
            },
            {
                "name": "PARENT_BAR",
                "value": "BAR",
                "type": "env"
            },
            {
                "name": "AGE",
                "value": "100",
                "type": "str"
            }
        ]);
        let node = RedEnvStoreBuilder::default().with_parent(&flow).load_json(&json).build();
        assert_eq!(node.evalute_env("MY_FOO").unwrap().as_str().unwrap(), "aaa");
        assert_eq!(node.evalute_env("${MY_FOO}").unwrap().as_str().unwrap(), "aaa");
        assert_eq!(node.evalute_env("GLOBAL_FOO").unwrap().as_str().unwrap(), "foofoo");
        assert_eq!(node.evalute_env("PARENT_BAR").unwrap().as_str().unwrap(), "barbar");
        assert_eq!(node.evalute_env("AGE").unwrap().as_str().unwrap(), "100");
        assert_eq!(node.evalute_env("FILE_SIZE").unwrap().as_i64().unwrap(), 123);
    }
}
