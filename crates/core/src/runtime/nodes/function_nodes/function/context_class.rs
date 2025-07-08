use rquickjs::{Ctx, Function, IntoJs, Value, class::Trace};
use rquickjs::{Exception, prelude::*};

use crate::runtime::context::Context as RedContext;
use crate::utils::async_util::SyncWaitableFuture;

use super::{UndefinableVariant, Variant};

#[derive(Clone, Trace)]
#[rquickjs::class(frozen)]
pub(super) struct ContextClass {
    #[qjs(skip_trace)]
    pub red_ctx: RedContext,
}

unsafe impl<'js> rquickjs::JsLifetime<'js> for ContextClass {
    type Changed<'to> = ContextClass;
}

#[allow(non_snake_case)]
#[rquickjs::methods]
impl ContextClass {
    #[qjs(skip)]
    pub fn new(red_ctx: RedContext) -> Self {
        ContextClass { red_ctx }
    }

    #[qjs(rename = "get")]
    pub fn get<'js>(
        self,
        keys: Value<'js>,
        store: Opt<rquickjs::String<'js>>,
        cb: Opt<Function<'js>>,
        ctx: Ctx<'js>,
    ) -> rquickjs::Result<Value<'js>> {
        let keys: String = keys.get()?;

        if let Some(cb) = cb.0 {
            let async_ctx = ctx.clone();
            // User provides the callback, we do it in async
            ctx.spawn(async move {
                let store = store.0.and_then(|x| x.get::<String>().ok());
                match self.red_ctx.get_one(store.as_deref(), keys.as_ref(), &[]).await {
                    Some(ctx_value) => {
                        let args = (Value::new_undefined(async_ctx.clone()), ctx_value.into_js(&async_ctx));
                        cb.call::<_, ()>(args).unwrap();
                    }
                    None => {
                        let args = (Value::new_undefined(async_ctx.clone()), Value::new_undefined(async_ctx.clone()));
                        cb.call::<_, ()>(args).unwrap();
                    }
                }
            });
            Ok(Value::new_undefined(ctx.clone()))
        } else {
            // No callback, we do it in sync
            let store = store.0.and_then(|x| x.get::<String>().ok());
            let ctx_value = async move { self.red_ctx.get_one(store.as_deref(), keys.as_ref(), &[]).await }.wait();
            UndefinableVariant(ctx_value).into_js(&ctx)
        }
    }

    #[qjs(rename = "set")]
    pub fn set<'js>(
        self,
        keys: Value<'js>,
        values: Value<'js>,
        store: Opt<rquickjs::String<'js>>,
        cb: Opt<Function<'js>>,
        ctx: Ctx<'js>,
    ) -> rquickjs::Result<()> {
        let keys: String = keys.get()?;
        let values: Variant = values.get()?;

        if let Some(cb) = cb.0 {
            let async_ctx = ctx.clone();
            // User provides the callback, we do it in async
            ctx.spawn(async move {
                let store = store.0.and_then(|x| x.get::<String>().ok());
                match self.red_ctx.set_one(store.as_deref(), keys.as_ref(), Some(values), &[]).await {
                    Ok(()) => {
                        let args = (Value::new_undefined(async_ctx.clone()),);
                        cb.call::<_, ()>(args).unwrap();
                    }
                    Err(_) => {
                        let args =
                            (Exception::from_message(async_ctx.clone(), "Failed to parse key").into_js(&async_ctx),);
                        cb.call::<_, ()>(args).unwrap();
                    }
                }
            });
        } else {
            // No callback, we do it in sync
            let store = store.0.and_then(|x| x.get::<String>().ok());
            async move { self.red_ctx.set_one(store.as_deref(), keys.as_ref(), Some(values), &[]).await }
                .wait()
                .map_err(|e| ctx.throw(format!("{e}").into_js(&ctx).unwrap()))?;
        }
        Ok(())
    }

    #[qjs(rename = "keys")]
    pub fn keys<'js>(
        self,
        store: Opt<rquickjs::String<'js>>,
        cb: Opt<Function<'js>>,
        ctx: Ctx<'js>,
    ) -> rquickjs::Result<Value<'js>> {
        let async_ctx = ctx.clone();
        if let Some(cb) = cb.0 {
            // User provides the callback, we do it in async
            ctx.spawn(async move {
                let store = store.0.and_then(|x| x.get::<String>().ok());
                match self.red_ctx.keys(store.as_deref()).await {
                    Some(ctx_keys) => {
                        let args = (Value::new_undefined(async_ctx.clone()), ctx_keys.into_js(&async_ctx));
                        cb.call::<_, ()>(args).unwrap();
                    }
                    None => {
                        let args = (Value::new_undefined(async_ctx.clone()), Value::new_undefined(async_ctx.clone()));
                        cb.call::<_, ()>(args).unwrap();
                    }
                }
            });
            Ok(Value::new_undefined(ctx.clone()))
        } else {
            // No callback, we do it in sync
            let store = store.0.and_then(|x| x.get::<String>().ok());
            match async move { self.red_ctx.keys(store.as_deref()).await }.wait() {
                Some(ctx_keys) => ctx_keys.into_js(&ctx),
                None => Ok(Value::new_undefined(ctx.clone())),
            }
        }
    }
}
