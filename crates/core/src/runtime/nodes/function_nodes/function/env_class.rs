use rquickjs::{Ctx, IntoJs, Result, Value, class::Trace};

use crate::runtime::red_env::*;

#[derive(Clone, Trace)]
#[rquickjs::class(frozen)]
pub(super) struct EnvClass {
    #[qjs(skip_trace)]
    pub envs: RedEnvs,
}

unsafe impl<'js> rquickjs::JsLifetime<'js> for EnvClass {
    type Changed<'to> = EnvClass;
}

#[allow(non_snake_case)]
#[rquickjs::methods]
impl<'js> EnvClass {
    #[qjs(skip)]
    pub fn new(envs: &RedEnvs) -> Self {
        EnvClass { envs: envs.clone() }
    }

    #[qjs()]
    fn get(&self, key: Value<'js>, ctx: Ctx<'js>) -> Result<Value<'js>> {
        let key: String = key.get()?;
        let res: Value<'js> = match self.envs.evalute_env(key.as_ref()) {
            Some(var) => var.into_js(&ctx)?,
            _ => Value::new_undefined(ctx),
        };
        Ok(res)
    }
}
