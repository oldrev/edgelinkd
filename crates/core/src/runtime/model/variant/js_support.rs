use super::*;

#[cfg(feature = "js")]
mod js {
    pub use rquickjs::*;
}

#[cfg(feature = "js")]
impl<'js> js::FromJs<'js> for Variant {
    fn from_js(_ctx: &js::Ctx<'js>, jv: js::Value<'js>) -> js::Result<Variant> {
        match jv.type_of() {
            js::Type::Undefined => Ok(Variant::Null),

            js::Type::Null => Ok(Variant::Null),

            js::Type::Bool => Ok(Variant::Bool(jv.get()?)),

            js::Type::Int => Ok(Variant::from(jv.get::<i64>()?)),

            js::Type::Float => Ok(Variant::from(jv.get::<f64>()?)),

            js::Type::String => Ok(Variant::String(jv.get()?)),

            js::Type::Symbol => Ok(Variant::String(jv.get()?)),

            js::Type::Array => {
                if let Some(arr) = jv.as_array() {
                    if let Some(buf) = arr.as_typed_array::<u8>() {
                        match buf.as_bytes() {
                            Some(bytes) => Ok(Variant::Bytes(bytes.to_vec())),
                            None => {
                                Err(js::Error::FromJs { from: "TypedArray<u8>", to: "Variant::Bytes", message: None })
                            }
                        }
                    } else {
                        let mut vec: Vec<Variant> = Vec::with_capacity(arr.len());
                        for item in arr.iter() {
                            match item {
                                Ok(v) => vec.push(Variant::from_js(_ctx, v)?),
                                Err(err) => {
                                    return Err(err);
                                }
                            }
                        }
                        Ok(Variant::Array(vec))
                    }
                } else {
                    Ok(Variant::Null)
                }
            }

            js::Type::Object => {
                if let Some(jo) = jv.as_object() {
                    let global = _ctx.globals();
                    let date_ctor: Constructor = global.get("Date")?;
                    let regexp_ctor: Constructor = global.get("RegExp")?;
                    if jo.is_instance_of(date_ctor) {
                        let st = jv.get::<SystemTime>()?;
                        Ok(Variant::Date(st))
                    } else if jo.is_instance_of(regexp_ctor) {
                        let to_string_fn: js::Function = jo.get("toString")?;
                        let re_str: String = to_string_fn.call((js::function::This(jv),))?;
                        match Regex::new(re_str.as_str()) {
                            Ok(re) => Ok(Variant::Regexp(re)),
                            Err(_) => Err(js::Error::FromJs {
                                from: "JS object",
                                to: "Variant::Regexp",
                                message: Some(format!("Failed to create Regex from: '{re_str}'")),
                            }),
                        }
                    } else if let Some(buf) = jo.as_array_buffer() {
                        match buf.as_bytes() {
                            Some(bytes) => Ok(Variant::Bytes(bytes.to_vec())),
                            None => Err(js::Error::FromJs { from: "ArrayBuffer", to: "Variant::Bytes", message: None }),
                        }
                    } else {
                        let mut map = VariantObjectMap::new();
                        for result in jo.props::<String, js::Value>() {
                            match result {
                                Ok((ref k, v)) => {
                                    map.insert(k.clone(), Variant::from_js(_ctx, v)?);
                                }
                                Err(e) => {
                                    log::error!("Unknown fatal error: {e}");
                                    unreachable!();
                                }
                            }
                        }
                        Ok(Variant::Object(map))
                    }
                } else {
                    Err(js::Error::FromJs { from: "JS object", to: "Variant::Object", message: None })
                }
            }

            _ => Err(js::Error::FromJs { from: "Unknown JS type", to: "", message: None }),
        }
    }
}

#[cfg(feature = "js")]
impl<'js> js::IntoJs<'js> for Variant {
    fn into_js(self, ctx: &js::Ctx<'js>) -> js::Result<js::Value<'js>> {
        use js::function::Constructor;

        match self {
            Variant::Array(arr) => arr.into_js(ctx),

            Variant::Bool(b) => b.into_js(ctx),

            Variant::Bytes(bytes) => Ok(js::ArrayBuffer::new(ctx.clone(), bytes)?.into_value()),

            Variant::Number(num) => {
                if let Some(f) = num.as_f64() {
                    f.into_js(ctx)
                } else if let Some(i) = num.as_i64() {
                    i.into_js(ctx)
                } else if let Some(u) = num.as_u64() {
                    u.into_js(ctx)
                } else {
                    unreachable!();
                }
            }

            Variant::Null => Ok(js::Value::new_null(ctx.clone())),

            Variant::Object(map) => map.into_js(ctx),

            Variant::String(s) => s.into_js(ctx),

            Variant::Date(t) => t.into_js(ctx),

            Variant::Regexp(re) => {
                let global = ctx.globals();
                let regexp_ctor: Constructor = global.get("RegExp")?;
                regexp_ctor.construct((re.as_str(),))
            }
        }
    }
}

#[cfg(feature = "js")]
impl<'js> js::IntoJs<'js> for UndefinableVariant {
    fn into_js(self, ctx: &js::Ctx<'js>) -> js::Result<js::Value<'js>> {
        match self.0 {
            Some(var) => var.into_js(ctx),
            None => Ok(js::Value::new_undefined(ctx.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use js::IntoJs;
    use serde_json::*;

    use super::*;

    #[test]
    fn variant_into_js() {
        let js_rt = js::Runtime::new().unwrap();
        let ctx = js::Context::full(&js_rt).unwrap();

        let foo = Variant::from(json!({
            "intValue": 123,
            "strValue": "hello",
            "arrayValue": [1, 2, 3],
        }));

        ctx.with(|ctx| {
            let globs = ctx.globals();
            globs.set("foo", foo.into_js(&ctx).unwrap()).unwrap();

            let v: i64 = ctx.eval("foo.intValue").unwrap();
            assert_eq!(v, 123);

            let v: std::string::String = ctx.eval("foo.strValue").unwrap();
            assert_eq!(v, "hello".to_owned());

            let v: Vec<i32> = ctx.eval("foo.arrayValue").unwrap();
            assert_eq!(v, vec![1, 2, 3]);

            let v: Vec<Variant> = ctx.eval("foo.arrayValue").unwrap();
            assert_eq!(v, vec![Variant::from(1), Variant::from(2), Variant::from(3)]);
        });
    }
}
