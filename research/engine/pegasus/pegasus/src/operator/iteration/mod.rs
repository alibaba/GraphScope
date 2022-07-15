use crate::api::{IterCondition, Iteration};
use crate::macros::filter::*;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

mod feedback;
mod switch;
use feedback::FeedbackOperator;
use switch::SwitchOperator;

use crate::api::iteration::EmitKind;
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
            iterate(self, until, None, func)
        }
    }

    fn iterate_until<F>(self, until: IterCondition<D>, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
    {
        iterate(self, until, None, func)
    }

    fn iterate_emit_until<F>(
        self, until: IterCondition<D>, emit_kind: EmitKind, func: F,
    ) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
    {
        iterate(self, until, Some(emit_kind), func)
    }
}

fn iterate<D, F>(
    stream: Stream<D>, until: IterCondition<D>, emit_kind: Option<EmitKind>, func: F,
) -> Result<Stream<D>, BuildJobError>
where
    D: Data,
    F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
{
    let max_iters = until.max_iters;
    let (leave, enter) = stream
        .enter()?
        .binary_branch_notify("switch", |info| {
            SwitchOperator::<D>::new(info.scope_level, emit_kind, until)
        })?;
    let index = enter.get_upstream_port().index;
    let after_body = func(enter)?;
    let feedback: Stream<D> = after_body
        .sync_state()
        .transform_notify("feedback", move |info| {
            FeedbackOperator::<D>::new(info.scope_level, max_iters)
        })?;
    feedback.feedback_to(index)?;
    leave.leave()
}

impl<D: 'static + Send> IterCondition<D> {
    pub fn until<F>(&mut self, func: F)
    where
        F: Fn(&D) -> FnResult<bool> + Send + 'static,
    {
        self.set_until(Box::new(filter!(func)));
    }
}
