use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, RemoveLabel, Step};
use crate::process::traversal::traverser::{Requirement, Traverser};
use crate::structure::Tag;
use crate::DynResult;
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

pub struct TransformTraverserStep {
    requirement: Requirement,
    remove_labels: Vec<Tag>,
}

impl TransformTraverserStep {
    pub fn new(requirement: Requirement) -> Self {
        TransformTraverserStep { requirement, remove_labels: vec![] }
    }
}

impl Step for TransformTraverserStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::To
    }

    fn add_tag(&mut self, _label: Tag) {
        unimplemented!()
    }

    fn tags(&self) -> &[Tag] {
        unimplemented!()
    }
}

impl RemoveLabel for TransformTraverserStep {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_labels.push(label);
    }

    fn remove_tags(&self) -> &[Tag] {
        self.remove_labels.as_slice()
    }
}

struct TransformTraverserFunc {
    requirement: Requirement,
    remove_labels: BitSet,
}

impl MapFunction<Traverser, Traverser> for TransformTraverserFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if !self.remove_labels.is_empty() {
            input.remove_labels(&self.remove_labels)
        }
        Ok(input.transform(self.requirement))
    }
}

impl MapFuncGen for TransformTraverserStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let remove_labels = self.get_remove_tags();
        Ok(Box::new(TransformTraverserFunc { requirement: self.requirement, remove_labels }))
    }
}
