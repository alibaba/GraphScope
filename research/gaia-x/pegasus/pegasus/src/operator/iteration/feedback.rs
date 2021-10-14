use crate::api::notification::{Cancel, End};
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::errors::JobExecError;
use crate::operator::{Notifiable, OperatorCore};
use crate::tag::tools::map::TidyTagMap;
use crate::Data;

#[allow(dead_code)]
pub(crate) struct FeedbackOperator<D: Data> {
    pub _scope_level: u32,
    worker_index: u32,
    max_iters: u32,
    observer: TidyTagMap<()>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> FeedbackOperator<D> {
    pub fn new(_scope_level: u32, max_iters: u32) -> Self {
        let worker_index = crate::worker_id::get_current_worker().index;
        FeedbackOperator {
            _scope_level,
            worker_index,
            max_iters,
            observer: TidyTagMap::new(_scope_level - 1),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D: Data> OperatorCore for FeedbackOperator<D> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0]);
        let output = new_output::<D>(&outputs[0]);
        input.for_each_batch(|batch| {
            let end = batch.take_end();
            let len = batch.len();
            if len > 0 {
                output.push_batch_mut(batch)?;
            }

            if let Some(mut end) = end {
                if end.total_send == 0 {
                    trace_worker!("no data of {:?} feedback into next iteration;", batch.tag);
                    end.tag = end.tag.advance_to(self.max_iters - 1);
                }
                if len == 0 && end.source.contains_source(self.worker_index) {
                    end.total_send = 0;
                }
                output.notify_end(end)?;
            }

            Ok(())
        })
    }
}

impl<D: Data> Notifiable for FeedbackOperator<D> {
    fn on_end(&mut self, n: End, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        let level = n.tag().len() as u32;
        if level == 0 {
            outputs[0].notify_end(n.take())?;
        } else {
            //ignore;
        }
        Ok(())
    }

    fn on_cancel(&mut self, n: Cancel, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
        inputs[0].cancel_scope(n.tag());
        Ok(())
    }
}
