use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::group_by::KeyFunctionGen;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::Details;
use crate::structure::Element;
use crate::structure::Token;
use pegasus::preclude::function::KeyFunction;
use pegasus_server::factory::HashKey;
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct GroupByStep {
    tag_key: TagKey,
}

impl GroupByStep {
    pub fn new(tag_key: TagKey) -> Self {
        GroupByStep { tag_key }
    }
}

impl Step for GroupByStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Group
    }

    fn add_tag(&mut self, _label: String) {
        unimplemented!()
    }

    fn tags(&self) -> &[String] {
        unimplemented!()
    }
}

pub struct GroupBy {
    tag_key: TagKey,
}

impl KeyFunction<Traverser> for GroupBy {
    type Target = HashKey<Traverser>;

    // TODO(longbin) Throw error instead of expect or panic
    fn get_key(&self, item: &Traverser) -> Cow<Self::Target> {
        let (tag, by_key) = (self.tag_key.tag.as_ref(), self.tag_key.by_key.as_ref());
        let mut group_by_self = false;
        let group_key = if let Some(key) = by_key {
            match key {
                // "a" or head should be a graph_element
                ByStepOption::OptToken(token) => {
                    let graph_element = if let Some(tag) = tag {
                        item.select_as_element(tag)
                            .expect(&format!("Select tag {:?} as element error!", tag))
                    } else {
                        item.get_element().expect("should be graph_element")
                    };
                    let group_key = match token {
                        // by select("a").by(id) or select(id)
                        Token::Id => graph_element.id().into(),
                        // by select("a").by(label) or select(label)
                        Token::Label => graph_element.label().into(),
                        // by select("a").by("name") or select("name")
                        Token::Property(prop_name) => graph_element
                            .details()
                            .get_property(&prop_name)
                            .expect("cannot get property")
                            .try_to_owned()
                            .expect("Can't get owned property value"),
                        // TODO: by select("a").by(valueMap("name")) or by(valueMap("name"))
                    };
                    group_key
                }
                ByStepOption::OptProperties(_) => unimplemented!(),
                // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                // TODO(bingqing): by(select("a").by(keys)), by(select("a").by(values)), by(by(keys)) or by(by(values))
                ByStepOption::OptGroupKeys | ByStepOption::OptGroupValues => unimplemented!(),
                // by a value computed in sub-traversal
                ByStepOption::OptSubtraversal => {
                    if tag.is_some() {
                        panic!("do not support tag when by_key is SubKey")
                    }
                    item.get_element()
                        .expect("should be graph_element")
                        .get_attached()
                        .expect("should with attached object")
                        .clone()
                }
                ByStepOption::OptInvalid => panic!("Invalid by option"),
            }
        } else {
            // by select("a") where "a" is a precomputed value
            if let Some(tag) = tag {
                item.select_as_value(tag)
                    .expect(&format!("Select tag {:?} as value error!", tag))
                    .clone()
            } else {
                group_by_self = true;
                item.get_element().expect("should be graph_element").id().into()
            }
        };

        let mut state = DefaultHasher::new();
        group_key.hash(&mut state);
        let hash = state.finish();
        let traverser = if group_by_self { item.clone() } else { Traverser::Unknown(group_key) };
        let hash_key = HashKey::new(hash, traverser);
        Cow::Owned(hash_key)
    }
}

impl KeyFunctionGen for GroupByStep {
    fn gen(&self) -> Box<dyn KeyFunction<Traverser, Target = HashKey<Traverser>>> {
        Box::new(GroupBy { tag_key: self.tag_key.clone() })
    }
}
