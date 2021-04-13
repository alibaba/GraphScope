use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, RemoveTag, Step};
use crate::process::traversal::traverser::{Requirement, Traverser};
use crate::structure::Tag;
use crate::DynResult;
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

pub struct TransformTraverserStep {
    requirement: Requirement,
    remove_tags: Vec<Tag>,
}

impl TransformTraverserStep {
    pub fn new(requirement: Requirement) -> Self {
        TransformTraverserStep { requirement, remove_tags: vec![] }
    }
}

impl Step for TransformTraverserStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::To
    }
}

impl RemoveTag for TransformTraverserStep {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_tags.push(label);
    }

    fn get_remove_tags_as_slice(&self) -> &[Tag] {
        self.remove_tags.as_slice()
    }
}

struct TransformTraverserFunc {
    requirement: Requirement,
    remove_tags: BitSet,
}

impl MapFunction<Traverser, Traverser> for TransformTraverserFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if !self.remove_tags.is_empty() {
            input.remove_tags(&self.remove_tags)
        }
        Ok(input.transform(self.requirement))
    }
}

impl MapFuncGen for TransformTraverserStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let remove_tags = self.get_remove_tags();
        Ok(Box::new(TransformTraverserFunc { requirement: self.requirement, remove_tags }))
    }
}
