use edgelink_macro::*;

use crate::runtime::engine::Engine;
use crate::runtime::model::json::RedGlobalNodeConfig;
use crate::runtime::nodes::*;
use crate::runtime::red_env::RedEnvStoreBuilder;

/// The `global-config` node: it carries the environment variables of the editor's workspace
/// settings (`env`).
///
/// Node-RED evaluates them into the *global* environment when the global flow starts
/// (`runtime/lib/flows/Flow.js`: "This is the global flow. It needs to go find the `global-config`
/// node ... and evaluate any env properties"), which is why the values end up at the bottom of the
/// environment stack that every node resolves through: node, then group, then flow, then engine.
#[derive(Debug)]
#[global_node("global-config", red_name = "global-config", module = "edgelink_core")]
struct GlobalConfigNode {
    base: BaseGlobalNodeState,
}

impl GlobalConfigNode {
    fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        if let Some(env_json) = config.rest.get("env") {
            // The builder evaluates the entries in dependency order, so an entry may refer to an
            // earlier one (`type: "env"`) and `type: "jsonata"` is evaluated here, at deploy time.
            let envs = RedEnvStoreBuilder::default().load_json(env_json).build();
            engine.add_envs(&envs);
        }

        let context = engine.get_context_manager().new_context(engine.context(), config.id.to_string());
        let node = Self {
            base: BaseGlobalNodeState {
                id: config.id,
                name: config.name.clone(),
                type_str: wellknown_names::GLOBAL_CONFIG_NODE,
                ordering: config.ordering,
                disabled: config.disabled,
                context,
            },
        };
        Ok(Box::new(node))
    }
}

impl GlobalNodeBehavior for GlobalConfigNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }
}
