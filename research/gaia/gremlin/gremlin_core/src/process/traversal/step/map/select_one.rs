use crate::process::traversal::path::PathItem;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use pegasus::api::function::{FnResult, MapFunction};
use std::collections::HashSet;

pub struct SelectOneStep {
    select_tag: String,
    as_labels: Vec<String>,
}

impl SelectOneStep {
    pub fn new(select_tag: String) -> Self {
        SelectOneStep { select_tag, as_labels: vec![] }
    }
}

impl Step for SelectOneStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Select
    }

    fn add_tag(&mut self, label: String) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[String] {
        self.as_labels.as_slice()
    }
}

struct SelectOneFunc {
    select_tag: String,
    labels: HashSet<String>,
}

impl MapFunction<Traverser, Traverser> for SelectOneFunc {
    fn exec(&self, input: Traverser) -> FnResult<Traverser> {
        if let Some(path_item) = input.select(&self.select_tag) {
            match path_item {
                PathItem::OnGraph(graph_element) => {
                    Ok(input.split(graph_element.clone(), &self.labels))
                }
                PathItem::Detached(obj) => Ok(input.split_with_value(obj.clone(), &self.labels)),
            }
        } else {
            panic!("Cannot get tag")
        }
    }
}

impl MapFuncGen for SelectOneStep {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>> {
        let labels = self.get_tags();
        Box::new(SelectOneFunc { select_tag: self.select_tag.clone(), labels })
    }
}
