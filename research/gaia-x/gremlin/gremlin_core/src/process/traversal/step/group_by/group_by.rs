use crate::generated::gremlin as pb;
use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::functions::KeyFunction;
use crate::process::traversal::step::group_by::KeyFunctionGen;
use crate::process::traversal::traverser::Traverser;
use crate::structure::{Details, Element, Token};
use crate::{str_to_dyn_error, DynResult, FromPb};
use pegasus::api::function::FnResult;

impl KeyFunction<Traverser, Traverser, Traverser> for KeyBy {
    fn select_key(&self, item: Traverser) -> FnResult<(Traverser, Traverser)> {
        let (tag, by_key) = (self.tag_key.tag.as_ref(), self.tag_key.by_key.as_ref());
        if let Some(key) = by_key {
            match key {
                // "a" or head should be a graph_element
                ByStepOption::OptToken(token) => {
                    let graph_element = item
                        .select_as_element(tag)
                        .ok_or(str_to_dyn_error("should be graph_element"))?;
                    let obj = match token {
                        // by select("a").by(id) or select(id)
                        Token::Id => graph_element.id().into(),
                        // by select("a").by(label) or select(label)
                        Token::Label => graph_element.label().as_object(),
                        // by select("a").by("name") or select("name")
                        Token::Property(prop_name) => graph_element
                            .details()
                            .get_property(&prop_name)
                            .ok_or(str_to_dyn_error("cannot get property"))?
                            .try_to_owned()
                            .ok_or(str_to_dyn_error("Can't get owned property value"))?,
                    };
                    Ok((Traverser::Object(obj), item))
                }
                // TODO: by select("a").by(valueMap("name")) or by(valueMap("name"))
                ByStepOption::OptProperties(_) => {
                    Err(str_to_dyn_error("Have not support by valueMap in group by yet"))?
                }
                // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                // TODO(bingqing): by(select("a").by(keys)), by(select("a").by(values)), by(by(keys)) or by(by(values))
                ByStepOption::OptGroupKeys(_) | ByStepOption::OptGroupValues(_) => {
                    Err(str_to_dyn_error("Have not support OptGroupKeys/Values in group by yet"))?
                }
                // by a value computed in sub-traversal
                ByStepOption::OptSubtraversal => {
                    let obj = item
                        .get_element()
                        .ok_or(str_to_dyn_error("should be graph_element"))?
                        .get_attached()
                        .ok_or(str_to_dyn_error("should with attached object"))?
                        .clone();
                    Ok((Traverser::Object(obj), item))
                }
            }
        } else {
            // by select("a") where "a" is a precomputed value
            if let Some(tag) = tag {
                let obj = item
                    .select_as_value(tag)
                    .ok_or(str_to_dyn_error("Select tag as value error!"))?
                    .clone();
                Ok((Traverser::Object(obj), item))
            } else {
                // group by self, no need to keep path
                if let Some(element) = item.get_element() {
                    Ok((Traverser::new(element.clone()), item))
                } else if let Some(object) = item.get_object() {
                    Ok((Traverser::Object(object.clone()), item))
                } else {
                    unreachable!()
                }
            }
        }
    }
}

pub struct KeyBy {
    pub tag_key: TagKey,
}

impl KeyFunctionGen for pb::GroupByStep {
    fn gen_key(self) -> DynResult<Box<dyn KeyFunction<Traverser, Traverser, Traverser>>> {
        let tag_key = if let Some(tag_key_pb) = self.key {
            TagKey::from_pb(tag_key_pb)?
        } else {
            TagKey::default()
        };

        // check tag_key
        let (tag, key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
        if let Some(key) = key {
            match key {
                ByStepOption::OptProperties(_) => {
                    Err(str_to_dyn_error("Have not support by valueMap in group by yet"))?;
                }
                ByStepOption::OptGroupKeys(_) | ByStepOption::OptGroupValues(_) => {
                    Err(str_to_dyn_error("Have not support OptGroupKeys/Values in group by yet"))?;
                }
                ByStepOption::OptSubtraversal => {
                    if tag.is_some() {
                        Err(str_to_dyn_error(
                            "Do not support tag when OptSubtraversal in group by",
                        ))?;
                    }
                }
                _ => {}
            }
        }

        Ok(Box::new(KeyBy { tag_key }))
    }
}
