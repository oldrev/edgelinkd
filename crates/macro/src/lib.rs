extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{
    DeriveInput, Ident, LitStr, Result, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

// 自定义解析结构，用于解析宏参数
struct NodeArgs {
    node_type: String,
    red_name: String,
    module: Option<String>,
    version: Option<String>,
    local: Option<bool>,
    user: Option<bool>,
}

impl Parse for NodeArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        // 解析第一个参数：字符串字面量
        let node_type: LitStr = input.parse()?;

        // 解析逗号
        input.parse::<Token![,]>()?;

        // 解析 red_name 标识符
        let red_name_ident: Ident = input.parse()?;
        if red_name_ident != "red_name" {
            return Err(syn::Error::new(red_name_ident.span(), "Expected 'red_name'"));
        }

        // 解析等号
        input.parse::<Token![=]>()?;

        // 解析红色名称字符串字面量
        let red_name: LitStr = input.parse()?;

        // 初始化可选参数
        let mut module = None;
        let mut version = None;
        let mut local = None;
        let mut user = None;

        // 解析可选的命名参数
        while !input.is_empty() {
            // 解析逗号
            input.parse::<Token![,]>()?;

            // 如果没有更多内容，跳出循环
            if input.is_empty() {
                break;
            }

            // 解析参数名
            let param_name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            match param_name.to_string().as_str() {
                "module" => {
                    let value: LitStr = input.parse()?;
                    module = Some(value.value());
                }
                "version" => {
                    let value: LitStr = input.parse()?;
                    version = Some(value.value());
                }
                "local" => {
                    let value: syn::LitBool = input.parse()?;
                    local = Some(value.value());
                }
                "user" => {
                    let value: syn::LitBool = input.parse()?;
                    user = Some(value.value());
                }
                _ => {
                    return Err(syn::Error::new(param_name.span(), format!("Unknown parameter: {param_name}")));
                }
            }
        }

        Ok(NodeArgs { node_type: node_type.value(), red_name: red_name.value(), module, version, local, user })
    }
}

#[proc_macro_attribute]
pub fn flow_node(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let struct_name = &input.ident;

    // 解析属性参数：#[flow_node("node_type", red_name = "red_name")]
    let args = parse_macro_input!(attr as NodeArgs);

    let node_type = &args.node_type;
    let red_name = &args.red_name;
    let module = args.module.as_deref().unwrap_or("node-red");
    let version = args.version.as_deref().unwrap_or("0.0.1");
    let local = args.local.unwrap_or(false);
    let user = args.user.unwrap_or(false);

    // 验证参数不为空
    if node_type.trim().is_empty() {
        panic!("node_type cannot be empty");
    }
    if red_name.trim().is_empty() {
        panic!("red_name cannot be empty");
    }

    let expanded = quote! {
        #input

        impl FlowsElement for #struct_name {
            fn id(&self) -> ElementId {
                self.get_base().id
            }

            fn name(&self) -> &str {
                &self.get_base().name
            }

            fn type_str(&self) -> &'static str {
                self.get_base().type_str
            }

            fn ordering(&self) -> usize {
                self.get_base().ordering
            }

            fn is_disabled(&self) -> bool {
                self.get_base().disabled
            }

            fn parent_element(&self) -> Option<ElementId> {
                self.get_base().flow.upgrade().map(|arc| arc.id())
            }

            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn get_path(&self) -> String {
                format!("{}/{}", self.get_base().flow.upgrade().unwrap().get_path(), self.id())
            }

        }

        impl ContextHolder for #struct_name {
            fn context(&self) -> &Context {
                &self.get_base().context
            }
        }

        ::inventory::submit! {
            MetaNode {
                kind: NodeKind::Flow,
                type_: #node_type,
                factory: NodeFactory::Flow(#struct_name::build),
                red_id: concat!("node-red/", #red_name),
                red_name: #red_name,
                module: #module,
                version: #version,
                local: #local,
                user: #user,
            }
        }
    }; // quote!

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn global_node(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let struct_name = &input.ident;

    // 解析属性参数：#[global_node("node_type", red_name = "red_name")]
    let args = parse_macro_input!(attr as NodeArgs);

    let node_type = &args.node_type;
    let red_name = &args.red_name;
    let module = args.module.as_deref().unwrap_or("node-red");
    let version = args.version.as_deref().unwrap_or("0.0.1");
    let local = args.local.unwrap_or(false);
    let user = args.user.unwrap_or(false);

    // 验证参数不为空
    if node_type.trim().is_empty() {
        panic!("node_type cannot be empty");
    }
    if red_name.trim().is_empty() {
        panic!("red_name cannot be empty");
    }

    let expanded = quote! {
        #input

        impl FlowsElement for #struct_name {
            fn id(&self) -> ElementId {
                self.get_base().id
            }

            fn name(&self) -> &str {
                &self.get_base().name
            }

            fn type_str(&self) -> &'static str {
                self.get_base().type_str
            }

            fn ordering(&self) -> usize {
                self.get_base().ordering
            }

            fn is_disabled(&self) -> bool {
                self.get_base().disabled
            }

            fn parent_element(&self) -> Option<ElementId> {
                // TODO change it to engine
                log::warn!("Cannot get the parent element in global node");
                None
            }

            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn get_path(&self) -> String {
                self.id().to_string()
            }
        }

        impl ContextHolder for #struct_name {
            fn context(&self) -> &Context {
                &self.get_base().context
            }
        }

        ::inventory::submit! {
            MetaNode {
                kind: NodeKind::Global,
                type_: #node_type,
                factory: NodeFactory::Global(#struct_name::build),
                red_id: concat!(#node_type, "/", #red_name),
                red_name: #red_name,
                module: #module,
                version: #version,
                local: #local,
                user: #user,
            }
        }

    }; // quote!
    TokenStream::from(expanded)
}
