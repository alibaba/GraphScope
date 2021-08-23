use crate::api::{IterCondition, Iteration};
use crate::macros::filter::*;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

mod feedback;
mod switch;
use feedback::{FeedbackOperator, IterSyncOperator};
use switch::SwitchOperator;

use crate::macros::map::FnResult;

impl<D: Data> Iteration<D> for Stream<D> {
    fn iterate<F>(self, max_iters: u32, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
    {
        if max_iters == 0 {
            Err("at least one iteration")?;
        }

        if max_iters == 1 {
            func(self)
        } else {
            let until = IterCondition::max_iters(max_iters);
            self.iterate_until(until, func)
        }
    }

    fn iterate_until<F>(self, until: IterCondition<D>, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
    {
        let max_iters = until.max_iters;
        let (leave, enter) = self
            .enter()?
            .binary_branch_notify("switch", |info| SwitchOperator::<D>::new(info.scope_level, until))?;

        let index = enter.port().index;
        let after_iter = func(enter)?;
        let (pipeline, sync): (Stream<D>, Stream<D>) =
            after_iter.binary_branch_notify("sync", |info| IterSyncOperator::<D>::new(info.scope_level))?;
        let sync = sync.broadcast();
        let feedback: Stream<D> = pipeline.union_notify_transform("feedback", sync, move |info| {
            FeedbackOperator::<D>::new(info.scope_level, max_iters)
        })?;
        feedback.feedback_to(index)?;
        leave.leave()
    }
}

impl<D: 'static + Send> IterCondition<D> {
    pub fn until<F>(&mut self, func: F)
    where
        F: Fn(&D) -> FnResult<bool> + Send + 'static,
    {
        self.set_until(Box::new(filter!(func)));
    }
}
