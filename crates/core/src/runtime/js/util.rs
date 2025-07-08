use std::collections::HashMap;

use rquickjs::function::{Constructor, This};
use rquickjs::{Ctx, Function, IntoJs, Value};

pub fn deep_clone<'js>(ctx: Ctx<'js>, obj: Value<'js>) -> rquickjs::Result<Value<'js>> {
    if let Some(obj_ref) = obj.as_object() {
        let globals = ctx.globals();
        let date_ctor: Constructor = globals.get("Date")?;
        if obj_ref.is_instance_of(&date_ctor) {
            let get_time_fn: Function = obj_ref.get("getTime")?;
            let time: i64 = get_time_fn.call((This(&obj),))?;
            return date_ctor.construct((time,));
        }

        if let Some(src_arr) = obj_ref.as_array() {
            let mut arr_copy = Vec::with_capacity(src_arr.len());
            for item in src_arr.iter() {
                let cloned = deep_clone(ctx.clone(), item?)?;
                arr_copy.push(cloned);
            }
            return arr_copy.into_js(&ctx);
        }

        {
            let mut obj_copy: HashMap<String, Value<'js>> = HashMap::with_capacity(obj_ref.len());
            let has_own_property_fn: Function = obj_ref.get("hasOwnProperty")?;
            for item in obj_ref.props::<String, Value<'js>>() {
                let (k, v) = item?;
                let has: bool = has_own_property_fn.call((This(&obj), k.as_str()))?;
                if has {
                    obj_copy.insert(k, deep_clone(ctx.clone(), v)?);
                }
            }
            obj_copy.into_js(&ctx)
        }
    } else {
        Ok(obj)
    }
}
