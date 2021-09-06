use std::rc::Rc;

use crate::api::notification::{CancelScope, EndScope};
use crate::api::IterCondition;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::communication::Output;
use crate::data::{MarkedData, MicroBatch};
use crate::errors::JobExecError;
use crate::operator::{Notifiable, OperatorCore};
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::Data;

pub(crate) struct SwitchOperator<D> {
    scope_level: u32,
    cond: IterCondition<D>,
    // record scopes in iteration;
    // e.g. if scope ( [0, 0], [1, 0] ) need iteration:
    // it records :
    //  [0] -> [(end of 0), (end of root)]
    //  [1] -> [(end of 1), (end of root)]
    //
    iter_scope: TidyTagMap<Vec<EndGuard>>,
}

impl<D> SwitchOperator<D> {
    pub fn new(scope_level: u32, cond: IterCondition<D>) -> Self {
        assert!(scope_level > 0);
        SwitchOperator { scope_level, cond, iter_scope: TidyTagMap::new(scope_level - 1) }
    }
}

impl<D: Data> OperatorCore for SwitchOperator<D> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        assert_eq!(inputs.len(), 2);
        let mut main = new_input_session::<D>(&inputs[0]);
        let leave = new_output::<D>(&outputs[0]);
        let enter = new_output::<D>(&outputs[1]);
        main.for_each_batch(|dataset| {
            if dataset.is_last() {
                let tag = dataset.tag.to_parent_uncheck();
                trace_worker!("{:?} into iteration at scope level {}", tag, self.scope_level);
                self.iter_scope.insert(tag, vec![]);
            }
            switch(dataset, &self.cond, leave, enter)
        })?;

        let mut feedback = new_input_session::<D>(&inputs[1]);
        feedback.for_each_batch(|dataset| {
            if !dataset.is_empty() && log_enabled!(log::Level::Trace) {
                trace_worker!("receive feedback data of scope {:?}", dataset.tag);
            }
            if dataset.tag.current_uncheck() >= self.cond.max_iters {
                // The data of last iteration;
                if !dataset.is_empty() {
                    let mut leave_session = leave.new_session(&dataset.tag)?;
                    for d in dataset.drain() {
                        leave_session.give(d)?;
                    }
                }
                // if it is the last batch of last iteration:
                if let Some(end) = dataset.take_end() {
                    let p = end.tag.to_parent_uncheck();
                    let mut ends = self
                        .iter_scope
                        .remove(&p)
                        .expect("unknown iteration scope;");
                    leave.notify_end(end)?;
                    for e in ends.drain(..) {
                        if let Some(end) = e.try_unwrap() {
                            if end.tag.is_root() {
                                assert!(self.iter_scope.is_empty());
                                debug_worker!(
                                    "all scopes out of iteration at scope level {};",
                                    self.scope_level
                                );
                                // use root to close iteration body;
                                enter.notify_end(end.clone())?;
                            }
                            trace_worker!("{:?} out of iteration", end.tag);
                            leave.notify_end(end)?;
                        } else {
                            // the end signal is owned by other scopes which doesn't finish;
                            // ignore;
                        }
                    }
                }
            } else {
                // data not of last iteration;
                if !dataset.is_empty() {
                    switch(dataset, &self.cond, leave, enter)?;
                } else {
                    if let Some(end) = dataset.take_end() {
                        let p = end.tag.to_parent_uncheck();
                        if self.iter_scope.contains_key(&p) {
                            enter.notify_end(end)?;
                        } else {
                            //
                            error_worker!(
                                "weird scope[{:?}] end in iteration {};",
                                end.tag,
                                self.scope_level
                            );
                            unreachable!(
                                "weird scope[{:?}] end in iteration {};",
                                end.tag, self.scope_level
                            );
                        }
                    } else {
                        error_worker!("both data and signal empty of {:?}", dataset.tag);
                        unreachable!("both data and signal empty of {:?}", dataset.tag);
                    }
                }
            }
            Ok(())
        })
    }
}

fn switch<D: Data>(
    dataset: &mut MicroBatch<D>, cond: &IterCondition<D>, leave: &Output<D>, enter: &Output<D>,
) -> Result<(), JobExecError> {
    if !dataset.is_last() {
        // not last batch;
        let mut leave_session = leave.new_session(&dataset.tag)?;
        let mut enter_session = enter.new_session(&dataset.tag)?;
        for d in dataset.drain() {
            if cond.is_converge(&d)? {
                leave_session.give(d)?;
            } else {
                enter_session.give(d)?;
            }
        }
    } else {
        // last batch of a scope;
        debug_worker!("stop to send data of the {:?};", &dataset.tag);
        if !dataset.is_empty() {
            let tag = dataset.tag();
            let mut leave_session = leave.new_session(&tag)?;
            let mut enter_session = enter.new_session(&tag)?;
            for item in dataset.drain_to_end() {
                match item {
                    MarkedData::Data(d) => {
                        if cond.is_converge(&d)? {
                            leave_session.give(d)?;
                        } else {
                            enter_session.give(d)?;
                        }
                    }
                    MarkedData::Marked(d, e) => {
                        if let Some(d) = d {
                            if cond.is_converge(&d)? {
                                enter_session.notify_end(e)?;
                                leave_session.give(d)?;
                            } else {
                                enter_session.give_last(d, e)?;
                            }
                        } else {
                            enter_session.notify_end(e)?;
                        }
                    }
                }
            }
        } else if let Some(end) = dataset.take_end() {
            enter.notify_end(end)?;
        } else {
            unreachable!("both data and signal empty of {:?}", dataset.tag);
        }
    }
    Ok(())
}

impl<D: Data> Notifiable for SwitchOperator<D> {
    // handle end signal of parent scope;
    fn on_notify(&mut self, n: EndScope, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        debug_worker!("on notify of {:?} on in port {}", n.tag(), n.port);
        let level = n.tag().len();
        assert!(level < self.scope_level as usize);

        if n.port == 0 {
            if self.iter_scope.is_empty() {
                if n.tag().is_root() {
                    debug_worker!("all scopes out of iteration at scope level {};", self.scope_level);
                    let end: EndSignal = n.into();
                    outputs[0].notify_end(end.clone())?;
                    outputs[1].notify_end(end)?;
                }
            } else {
                let end = EndGuard::new(n.into());
                for (t, v) in self.iter_scope.iter_mut() {
                    if &end.end.tag == &*t || end.end.tag.is_parent_of(&t) {
                        v.push(end.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn on_cancel(&mut self, n: CancelScope, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
        if n.port == 0 {
            // received from outer loop
            assert!(n.tag().len() < self.scope_level as usize);
            trace_worker!("EARLY_STOP: cancel all iterations of scope {:?};", n.tag());
            for input in inputs.iter() {
                input.cancel_scope(n.tag());
            }
        } else {
            assert_eq!(n.port, 1);
            if n.tag().len() == self.scope_level as usize {
                let nth = n.tag().current_uncheck();
                // in the middle iteration, should propagated into previous iteration
                if nth != 0 {
                    trace_worker!("EARLY_STOP: cancel the {}th iteration of {:?};", nth, n.tag());
                    inputs[1].cancel_scope(n.tag());
                } else {
                    let p = n.tag().to_parent_uncheck();
                    trace_worker!("EARLY_STOP: cancel new iteration of {:?};", p);
                    inputs[0].cancel_scope(&p);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EndGuard {
    end: Rc<EndSignal>,
}

impl EndGuard {
    fn new(end: EndSignal) -> Self {
        EndGuard { end: Rc::new(end) }
    }

    fn try_unwrap(self) -> Option<EndSignal> {
        Rc::try_unwrap(self.end).ok()
    }
}

unsafe impl Send for EndGuard {}
