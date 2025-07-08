use edgelink_core::runtime::nodes::MetaNode;
use edgelink_core::runtime::registry::{RegistryBuilder, RegistryHandle};
use std::collections::BTreeMap;

// Type aliases to simplify complex nested types
type NodeEntry<'a> = (&'a str, &'a MetaNode);
type NodeList<'a> = Vec<NodeEntry<'a>>;
type RedNameMap<'a> = BTreeMap<&'a str, NodeList<'a>>;
type ModuleMap<'a> = BTreeMap<&'a str, RedNameMap<'a>>;

pub fn create_registry() -> edgelink_core::Result<RegistryHandle> {
    log::info!("Discovering all nodes...");
    // edgelink_core::runtime::registry::collect_nodes();
    log::info!("Loading node registry...");
    RegistryBuilder::default().build()
}

pub async fn list_available_nodes() -> anyhow::Result<()> {
    // Create a registry to discover all nodes
    let registry = RegistryBuilder::default().build()?;
    let all_nodes = registry.all();

    println!("Available Node Types in EdgeLink:");
    println!("==================================");

    // Group nodes by module first, then by red_name
    let mut modules: ModuleMap = BTreeMap::new();

    for (type_name, meta_node) in all_nodes.iter() {
        modules
            .entry(meta_node.module)
            .or_default()
            .entry(meta_node.red_name)
            .or_default()
            .push((type_name, meta_node));
    }

    for (module, red_names) in modules.iter() {
        println!("\nModule: `{module}`\n");

        for (red_name, nodes) in red_names.iter() {
            // Sort nodes by type_name within each group
            let mut sorted_nodes = nodes.clone();
            sorted_nodes.sort_by_key(|(type_name, _)| *type_name);

            println!("{red_name}:");

            for (_, meta_node) in sorted_nodes {
                // Build flags string
                let mut flags = Vec::new();
                if meta_node.local {
                    flags.push("local");
                }
                if meta_node.user {
                    flags.push("user");
                }
                let flags_str = if flags.is_empty() { String::new() } else { format!("[{}]", flags.join(", ")) };

                println!(
                    "\t{:<26} {}/{}\t\t{} {}",
                    meta_node.red_name, meta_node.module, meta_node.type_, meta_node.version, flags_str
                );
            }
        }
    }

    println!("\nTotal: {} node types available", all_nodes.len());

    Ok(())
}
