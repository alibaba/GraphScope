use crate::dataflow::Dataflow;
use crate::errors::{ErrorKind, JobExecError};
use crate::schedule::StepStrategy;

#[derive(Default)]
pub(super) struct WaterfallStrategy {
    markers: Vec<bool>,
}

impl WaterfallStrategy {
    #[inline]
    fn is_fired(&self, index: usize) -> bool {
        if self.markers.len() <= index {
            false
        } else {
            self.markers[index]
        }
    }

    #[inline]
    fn mark_fired(&mut self, index: usize) {
        while index >= self.markers.len() {
            self.markers.push(false);
        }
        self.markers[index] = true;
    }

    fn fire(&self, task: &Dataflow, index: usize) -> Result<bool, JobExecError> {
        loop {
            match task.try_fire(index) {
                Ok(fired) => return Ok(fired),
                Err(e) => match &e.kind {
                    ErrorKind::WouldBlock(tag) => {
                        if let Some(tag) = tag {
                            debug_worker!("scope {:?} blocked in operator {}", tag, index);
                        } else {
                            break;
                        }
                    }
                    ErrorKind::Interrupted => break,
                    _ => return Err(e),
                },
            }
        }
        Ok(true)
    }

    fn fire_follows(&mut self, start: usize, task: &Dataflow) -> Result<(), JobExecError> {
        let dependency = task.dependency();
        if let Some(children) = dependency.get_children_of(start) {
            for f in children.iter() {
                for (ff, _, _) in f.iter() {
                    if !self.is_fired(*ff) {
                        self.fire(task, *ff)?;
                        self.mark_fired(*ff);
                        self.fire_follows(*ff, task)?;
                    }
                }
            }
        } else {
            // debug_worker!("no child found of {}", start);
        }
        Ok(())
    }
}

impl StepStrategy for WaterfallStrategy {
    fn make_step(&mut self, task: &Dataflow) -> Result<(), JobExecError> {
        for x in self.markers.iter_mut() {
            *x = false;
        }
        self.fire_follows(0, task)?;
        Ok(())
    }
}

#[allow(dead_code)]
pub struct VolcanoStepStrategy {}

impl StepStrategy for VolcanoStepStrategy {
    fn make_step(&mut self, _task: &Dataflow) -> Result<(), JobExecError> {
        todo!()
    }
}
