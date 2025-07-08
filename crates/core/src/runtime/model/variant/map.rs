use std::borrow::Cow;

use super::*;

pub type VariantObjectMap = BTreeMap<String, Variant>;

pub trait VariantObject {
    fn contains_property(&self, prop: &str) -> bool;
    fn get_property(&self, prop: &str) -> Option<&Variant>;
    fn get_property_mut(&mut self, prop: &str) -> Option<&mut Variant>;
    fn get_nav_property(&self, expr: &str, eval_env: &[PropexEnv]) -> Option<&Variant>;
    fn get_nav_property_mut(&mut self, expr: &str, eval_env: &[PropexEnv]) -> Option<&mut Variant>;
    fn set_property(&mut self, prop: String, value: Variant);
    fn set_nav_property(
        &mut self,
        expr: &str,
        value: Variant,
        eval_env: &[PropexEnv],
        create_missing: bool,
    ) -> crate::Result<()>;

    fn get_segs_property(&self, segs: &[PropexSegment]) -> Option<&Variant>;
    fn get_segs_property_mut(&mut self, segs: &[PropexSegment]) -> Option<&mut Variant>;

    fn expand_segs_property(&self, segs: &mut [PropexSegment], eval_env: &[PropexEnv]) -> crate::Result<()>;

    fn remove_property(&mut self, prop: &str) -> Option<Variant>;
    fn remove_nav_property(&mut self, expr: &str, eval_env: &[PropexEnv]) -> Option<Variant>;
    fn remove_segs_property(&mut self, segs: &[PropexSegment]) -> Option<Variant>;
}

impl VariantObject for VariantObjectMap {
    fn contains_property(&self, prop: &str) -> bool {
        self.contains_key(prop)
    }

    fn get_property(&self, prop: &str) -> Option<&Variant> {
        self.get(prop)
    }

    fn get_property_mut(&mut self, prop: &str) -> Option<&mut Variant> {
        self.get_mut(prop)
    }

    /// Get the value of a navigation property
    ///
    /// The first level of the property expression for 'msg' must be a string, which means it must be
    /// `msg[msg.topic]` `msg['aaa']` or `msg.aaa`, and not `msg[12]`
    fn get_nav_property(&self, expr: &str, eval_env: &[PropexEnv]) -> Option<&Variant> {
        let mut segs = propex::parse(expr).ok()?;
        self.expand_segs_property(&mut segs, eval_env).ok()?;
        self.get_segs_property(&segs)
    }

    fn get_nav_property_mut(&mut self, expr: &str, eval_env: &[PropexEnv]) -> Option<&mut Variant> {
        let mut segs = propex::parse(expr).ok()?;
        self.expand_segs_property(&mut segs, eval_env).ok()?;
        self.get_segs_property_mut(&segs)
    }

    /// Set the value of a direct property.
    fn set_property(&mut self, prop: String, value: Variant) {
        let _ = self.insert(prop, value);
    }

    /// Set the value of a navigation property.
    fn set_nav_property(
        &mut self,
        expr: &str,
        value: Variant,
        eval_env: &[PropexEnv],
        create_missing: bool,
    ) -> crate::Result<()> {
        if expr.is_empty() {
            return Err(crate::EdgelinkError::BadArgument("expr"))
                .with_context(|| "The argument expr cannot be empty".to_owned());
        }

        let mut segs = propex::parse(expr).map_err(|_| crate::EdgelinkError::BadArgument("expr"))?;
        self.expand_segs_property(&mut segs, eval_env)?;

        let first_prop_name = match segs.first() {
            Some(PropexSegment::Property(name)) => name,
            _ => {
                return Err(crate::EdgelinkError::BadArgument("expr"))
                    .with_context(|| format!("The first property to access must be a string, but got '{expr}'"));
            }
        };

        // If create_missing is true and first_prop doesn't exist, we should create it here.
        let first_prop = match (self.get_property_mut(first_prop_name), create_missing, segs.len()) {
            (Some(prop), _, _) => prop,
            (None, true, 1) => {
                // Only one level of the property
                self.insert(expr.into(), value);
                return Ok(());
            }
            (None, true, _) => {
                let next_seg = segs.get(1);
                let var = match next_seg {
                    // the next level property is an object
                    Some(PropexSegment::Property(_)) => Variant::empty_object(),
                    Some(PropexSegment::Index(_)) => Variant::empty_array(),
                    _ => {
                        return Err(crate::EdgelinkError::BadArgument("expr"))
                            .with_context(|| format!("Not allowed to set first property: '{first_prop_name}'"));
                    }
                };
                self.insert(first_prop_name.to_string(), var);
                self.get_property_mut(first_prop_name).unwrap()
            }
            (None, _, _) => {
                return Err(crate::EdgelinkError::BadArgument("expr"))
                    .with_context(|| format!("Failed to set first property: '{first_prop_name}'"));
            }
        };

        if segs.len() == 1 {
            *first_prop = value;
            return Ok(());
        }

        match first_prop.get_segs_mut(&segs[1..]) {
            Some(pv) => {
                *pv = value;
                Ok(())
            }
            None if create_missing => first_prop.set_segs_property(&segs[1..], value, true),
            None => Err(crate::EdgelinkError::InvalidOperation(
                "Unable to set property: missing intermediate segments".into(),
            )
            .into()),
        }
    }

    fn get_segs_property(&self, segs: &[PropexSegment]) -> Option<&Variant> {
        match segs {
            [PropexSegment::Property(first_prop_name)] => self.get(first_prop_name.as_ref()),
            [PropexSegment::Property(first_prop_name), rest @ ..] => self.get(first_prop_name.as_ref())?.get_segs(rest),
            _ => None,
        }
    }

    fn get_segs_property_mut(&mut self, segs: &[PropexSegment]) -> Option<&mut Variant> {
        match segs {
            [PropexSegment::Property(first_prop_name)] => self.get_property_mut(first_prop_name),
            [PropexSegment::Property(first_prop_name), rest @ ..] => {
                self.get_property_mut(first_prop_name)?.get_segs_mut(rest)
            }
            _ => None,
        }
    }

    fn expand_segs_property(&self, segs: &mut [PropexSegment], eval_env: &[PropexEnv]) -> crate::Result<()> {
        for seg in segs.iter_mut() {
            if let PropexSegment::Nested(nested_segs) = seg {
                let nested_var = match nested_segs.first() {
                    Some(PropexSegment::Property(s)) => eval_env.find_seg(s).and_then(|x| match x {
                        PropexEnv::ThisRef(_) => Some(self),
                        PropexEnv::ExtRef(_, _) => None,
                    }),
                    // 不支持递归
                    _ => return Err(EdgelinkError::OutOfRange.into()),
                };
                if let Some(nested_var) = nested_var {
                    *seg = match nested_var.get_segs_property(&nested_segs[1..]).ok_or(EdgelinkError::OutOfRange)? {
                        Variant::String(str_index) => PropexSegment::Property(Cow::Owned(str_index.clone())),
                        Variant::Number(num_index)
                            if (num_index.is_u64() || num_index.is_i64()) && num_index.as_u64() >= Some(0) =>
                        {
                            PropexSegment::Index(num_index.as_u64().unwrap() as usize)
                        }
                        _ => return Err(EdgelinkError::OutOfRange.into()), // We cannot found the nested property
                    };
                } else {
                    return Err(EdgelinkError::OutOfRange.into());
                }
            }
        }
        Ok(())
    }

    fn remove_property(&mut self, prop: &str) -> Option<Variant> {
        self.remove(prop)
    }

    /// Remove the value of a navigation property.
    fn remove_nav_property(&mut self, expr: &str, eval_env: &[PropexEnv]) -> Option<Variant> {
        // Return None if the expression is empty.
        if expr.is_empty() {
            return None;
        }

        // Parse the expression into segments.
        // TODO nested
        let mut path = propex::parse(expr).ok()?;
        self.expand_segs_property(&mut path, eval_env).ok()?;

        self.remove_segs_property(&path)
    }

    fn remove_segs_property(&mut self, segs: &[PropexSegment]) -> Option<Variant> {
        // Return None if the expression is empty.
        if segs.is_empty() {
            return None;
        }
        // Handle the parsed segments.
        match segs {
            // If there's only one segment, remove the property directly.
            [PropexSegment::Property(first_prop_name)] => self.remove(first_prop_name.as_ref()),

            // If there are multiple segments, navigate through the nested structure.
            [PropexSegment::Property(first_prop_name), rest @ ..] => {
                // Get the mutable reference to the navigation property.
                let prop_tail = self.get_mut(first_prop_name.as_ref())?.get_segs_mut(&rest[..rest.len() - 1])?;

                // Remove the value based on the type of the last segment.
                match (prop_tail, segs.last()?) {
                    (Variant::Object(tail_map), PropexSegment::Property(tail_seg)) => {
                        tail_map.remove(tail_seg.as_ref())
                    }
                    (Variant::Array(tail_arr), PropexSegment::Index(tail_index)) => Some(tail_arr.remove(*tail_index)),
                    _ => None,
                }
            }

            // If the segments don't match the expected pattern, return None.
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_nav_property() {
        let mut obj1 = Variant::from([
            ("value1", Variant::from(123)),
            ("value2", Variant::from(123.0)),
            (
                "value3",
                Variant::from([
                    ("aaa", Variant::from(333)),
                    ("bbb", Variant::from(444)),
                    ("ccc", Variant::from(555)),
                    ("ddd", Variant::from(999)),
                ]),
            ),
            ("value4", Variant::Array(vec!["foo".into(), "foobar".into(), "bar".into()])),
        ])
        .as_object()
        .cloned()
        .unwrap();

        assert!(obj1.get("value3").unwrap().as_object().unwrap().contains_key("aaa"));
        let _ = obj1.remove_nav_property("value3.aaa", &[]).unwrap();
        assert!(!obj1.get("value3").unwrap().as_object().unwrap().contains_key("aaa"));

        assert!(obj1.get("value4").unwrap().as_array().unwrap().contains(&Variant::String("foobar".into())));
        assert_eq!(obj1.get("value4").unwrap().as_array().unwrap().len(), 3);
        let _ = obj1.remove_nav_property("value4[1]", &[]).unwrap();
        assert!(!obj1.get("value4").unwrap().as_array().unwrap().contains(&Variant::String("foobar".into())));
        assert_eq!(obj1.get("value4").unwrap().as_array().unwrap().len(), 2);
    }
}
