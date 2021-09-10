use crate::api::notification::{CancelScope, EndScope};
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::errors::JobExecError;
use crate::operator::{Notifiable, OperatorCore};
use crate::progress::{EndSignal, Weight};
use crate::tag::tools::map::TidyTagMap;
use crate::Data;

pub(crate) struct IterSyncOperator<D: Data> {
    observer: TidyTagMap<usize>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> IterSyncOperator<D> {
    pub fn new(scope_level: u32) -> Self {
        IterSyncOperator { observer: TidyTagMap::new(scope_level), _ph: std::marker::PhantomData }
    }
}

impl<D: Data> OperatorCore for IterSyncOperator<D> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0]);
        let output = new_output::<D>(&outputs[0]);
        input.for_each_batch(|dataset| {
            if dataset.is_last() {
                let mut count = self.observer.remove(&dataset.tag).unwrap_or(0);
                if !dataset.is_empty() {
                    count += 1;
                    output.push_batch_mut(dataset)?;
                }

                if let Some(end) = dataset.take_end() {
                    if count == 0 {
                        let sync = end.tag.clone();
                        debug_worker!("detect if termination on {:?}", sync);
                        let end = EndSignal::new(sync, Weight::all());
                        outputs[1].notify_end(end)?;
                    }
                    output.notify_end(end)?;
                }
            } else {
                if !dataset.is_empty() {
                    let cnt = self
                        .observer
                        .get_mut_or_else(&dataset.tag, || 0);
                    *cnt += 1;
                    output.push_batch_mut(dataset)?;
                }
            }
            Ok(())
        })
    }
}

impl<D: Data> Notifiable for IterSyncOperator<D> {
    fn on_notify(&mut self, n: EndScope, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        let end: EndSignal = n.into();
        outputs[0].notify_end(end.clone())?;
        outputs[1].notify_end(end)?;
        Ok(())
    }

    fn on_cancel(&mut self, n: CancelScope, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
        assert_eq!(n.port, 0);
        inputs[0].cancel_scope(&n.tag);
        Ok(())
    }
}

pub(crate) struct FeedbackOperator<D: Data> {
    pub _scope_level: u32,
    max_iters: u32,
    observer: TidyTagMap<()>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> FeedbackOperator<D> {
    pub fn new(_scope_level: u32, max_iters: u32) -> Self {
        FeedbackOperator {
            _scope_level,
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
        input.for_each_batch(|dataset| {
            let mut session = output.new_session(&dataset.tag)?;
            for d in dataset.drain() {
                session.give(d)?;
            }

            if let Some(end) = dataset.take_end() {
                assert!(end.tag.len() > 0);
                let cur = end.tag.current_uncheck();
                let p = end.tag.to_parent_uncheck();
                if cur == 0 {
                    debug_worker!("observe {:?} in iteration;", p);
                    self.observer.insert(p, ());
                    session.notify_end(end)?;
                } else if cur == self.max_iters - 1 {
                    debug_worker!("observe {:?} out iteration;", p);
                    self.observer.remove(&p);
                    session.notify_end(end)?;
                } else if self.observer.contains_key(&p) {
                    session.notify_end(end)?;
                } else {
                    //
                    debug_worker!("redundant end {:?}, ignore;", end.tag);
                }
            }

            Ok(())
        })?;

        let mut sync_input = new_input_session::<D>(&inputs[1]);
        sync_input.for_each_batch(|dataset| {
            assert!(dataset.is_empty());
            assert!(dataset.is_last());
            if let Some(mut end) = dataset.take_end() {
                let p = end.tag.to_parent_uncheck();
                if let Some(_) = self.observer.remove(&p) {
                    debug_worker!(
                        "observe {:?} terminate on {}th iteration;",
                        p,
                        end.tag.current_uncheck()
                    );
                    end.tag = end.tag.advance_to(self.max_iters - 1);
                    output.notify_end(end)?;
                }
            }
            Ok(())
        })
    }
}

impl<D: Data> Notifiable for FeedbackOperator<D> {
    fn on_notify(&mut self, n: EndScope, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        trace_worker!("feedback: on notify of {:?} on {:?}", n.tag(), n.port);
        if n.port == 0 {
            if n.tag().is_root() {
                outputs[0].notify_end(n.into())?;
            }
        }
        Ok(())
    }

    fn on_cancel(&mut self, n: CancelScope, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
        assert_eq!(n.port, 0);
        inputs[0].cancel_scope(n.tag());
        Ok(())
    }
}
