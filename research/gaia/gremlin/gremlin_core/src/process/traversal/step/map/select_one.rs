use crate::process::traversal::path::PathItem;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, RemoveLabel, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::Tag;
use crate::{str_to_dyn_error, DynResult};
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

pub struct SelectOneStep {
    select_tag: Tag,
    as_labels: Vec<Tag>,
    remove_labels: Vec<Tag>,
}

impl SelectOneStep {
    pub fn new(select_tag: Tag) -> Self {
        SelectOneStep { select_tag, as_labels: vec![], remove_labels: vec![] }
    }
}

impl Step for SelectOneStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Select
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[Tag] {
        self.as_labels.as_slice()
    }
}

impl RemoveLabel for SelectOneStep {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_labels.push(label);
    }

    fn remove_tags(&self) -> &[Tag] {
        self.remove_labels.as_slice()
    }
}

struct SelectOneFunc {
    select_tag: Tag,
    labels: BitSet,
    remove_labels: BitSet,
}

impl MapFunction<Traverser, Traverser> for SelectOneFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if let Some(path_item) = input.select(&self.select_tag) {
            match path_item {
                PathItem::OnGraph(graph_element) => {
                    let graph_element = graph_element.clone();
                    input.split(graph_element, &self.labels);
                    input.remove_labels(&self.remove_labels);
                    Ok(input)
                }
                PathItem::Detached(obj) => {
                    let obj = obj.clone();
                    input.split_with_value(obj, &self.labels);
                    input.remove_labels(&self.remove_labels);
                    Ok(input)
                }
                PathItem::Empty => {
                    Err(str_to_dyn_error("Cannot get tag since the item is already deleted"))
                }
            }
        } else {
            Err(str_to_dyn_error("Cannot get tag"))
        }
    }
}

impl MapFuncGen for SelectOneStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let labels = self.get_tags();
        let remove_labels = self.get_remove_tags();
        Ok(Box::new(SelectOneFunc { select_tag: self.select_tag.clone(), labels, remove_labels }))
    }
}
