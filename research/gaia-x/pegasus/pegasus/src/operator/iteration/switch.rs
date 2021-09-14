use pegasus_common::rc::UnsafeRcPtr;

use crate::api::IterCondition;
use crate::data::EndByScope;
use crate::tag::tools::map::TidyTagMap;

#[derive(Clone)]
struct EndGuard {
    end: UnsafeRcPtr<EndByScope>,
}

impl EndGuard {
    fn new(end: EndByScope) -> Self {
        EndGuard { end: UnsafeRcPtr::new(end) }
    }

    fn try_unwrap(self) -> Option<EndByScope> {
        UnsafeRcPtr::try_unwrap(self.end).ok()
    }
}

pub(crate) struct SwitchOperator<D> {
    scope_level: u32,
    cond: IterCondition<D>,
    // record scopes in iteration;
    // e.g. if scope ( [0, 0], [1, 0] ) need iteration:
    // it records :
    //  [0] -> [(end of 0), (end of root)]
    //  [1] -> [(end of 1), (end of root)]
    //
    parent_scope_ends: TidyTagMap<Vec<EndGuard>>,
    in_iteration: TidyTagMap<()>,
    exhaust_port: Option<EndByScope>,
}

impl<D> SwitchOperator<D> {
    pub fn new(scope_level: u32, cond: IterCondition<D>) -> Self {
        assert!(scope_level > 0);
        SwitchOperator {
            scope_level,
            cond,
            parent_scope_ends: TidyTagMap::new(scope_level - 1),
            in_iteration: TidyTagMap::new(scope_level - 1),
            exhaust_port: None,
        }
    }
}

#[cfg(not(feature = "rob"))]
mod rob {
    use crate::communication::input::{new_input_session, InputProxy};
    use crate::communication::output::{new_output, OutputProxy};
    use crate::communication::Output;
    use crate::data::{EndByScope, MarkedData, MicroBatch};
    use crate::errors::JobExecError;
    use crate::operator::{Notifiable, OperatorCore};
    use crate::tag::tools::map::TidyTagMap;
    use crate::Data;

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
        fn on_end(&mut self, n: End, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
            debug_worker!("on notify of {:?} on in port {}", n.tag(), n.port);
            let level = n.tag().len();
            assert!(level < self.scope_level as usize);

            if n.port == 0 {
                if self.iter_scope.is_empty() {
                    if n.tag().is_root() {
                        debug_worker!("all scopes out of iteration at scope level {};", self.scope_level);
                        let end = n.take();
                        outputs[0].notify_end(end.clone())?;
                        outputs[1].notify_end(end)?;
                    }
                } else {
                    let end = EndGuard::new(n.take());
                    for (t, v) in self.iter_scope.iter_mut() {
                        if &end.end.tag == &*t || end.end.tag.is_parent_of(&t) {
                            v.push(end.clone());
                        }
                    }
                }
            }
            Ok(())
        }

        fn on_cancel(&mut self, n: Cancel, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
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
}

#[cfg(feature = "rob")]
mod rob {
    use super::*;
    use crate::api::notification::{Cancel, End};
    use crate::communication::input::{new_input_session, InputProxy};
    use crate::communication::output::{new_output, OutputProxy};
    use crate::errors::JobExecError;
    use crate::operator::{Notifiable, OperatorCore};
    use crate::Data;

    impl<D: Data> OperatorCore for SwitchOperator<D> {
        fn on_receive(
            &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
        ) -> Result<(), JobExecError> {
            let leave = new_output::<D>(&outputs[0]);
            let enter = new_output::<D>(&outputs[1]);
            let mut main = new_input_session::<D>(&inputs[0]);
            main.for_each_batch(|batch| {
                let mut leave = leave.new_session(&batch.tag)?;
                let mut enter = enter.new_session(&batch.tag)?;
                for data in batch.drain() {
                    if self.cond.is_converge(&data)? {
                        leave.give(data)?;
                    } else {
                        enter.give(data)?;
                    }
                }

                if let Some(end) = batch.take_end() {
                    let p = batch.tag.to_parent_uncheck();
                    trace_worker!("detect scope {:?} in iteration;", batch.tag);
                    self.in_iteration.insert(p, ());
                    enter.notify_end(end)?;
                }

                Ok(())
            })?;

            let mut feedback = new_input_session::<D>(&inputs[1]);
            feedback.for_each_batch(|batch| {
                if batch.tag.current_uncheck() >= self.cond.max_iters {
                    let end = batch.take_end();
                    if !batch.is_empty() {
                        leave.push_batch_mut(batch)?;
                    }
                    if let Some(_) = end {
                        trace_worker!("detect {:?} leave iteration;", batch.tag);
                        let p = batch.tag.to_parent_uncheck();
                        if self.in_iteration.remove(&p).is_some() {
                            if let Some(ends) = self.parent_scope_ends.remove(&p) {
                                for e in ends {
                                    if let Some(end) = e.try_unwrap() {
                                        leave.notify_end(end.clone())?;
                                        enter.notify_end(end)?;
                                    }
                                }
                            }
                            if self.in_iteration.is_empty() {
                                if self.parent_scope_ends.is_empty() {
                                    if let Some(ref end) = self.exhaust_port {
                                        enter.notify_end(end.clone())?;
                                    }
                                } else {
                                    error_worker!("no scope in iteration, parent scope end size = {};", self.parent_scope_ends.len());
                                    for (x, _) in self.parent_scope_ends.iter() {
                                        error_worker!("parent scope {:?} end exist;", x);
                                    }
                                }
                            }
                        } else {
                            error_worker!("iteration for {:?} not found;", p);
                        }
                    }
                } else {
                    let mut leave = leave.new_session(&batch.tag)?;
                    let mut enter = enter.new_session(&batch.tag)?;
                    for data in batch.drain() {
                        if self.cond.is_converge(&data)? {
                            leave.give(data)?;
                        } else {
                            enter.give(data)?;
                        }
                    }
                    if let Some(end) = batch.take_end() {
                        let p = batch.tag.to_parent_uncheck();
                        if self.in_iteration.insert(p, ()).is_none() {
                            trace_worker!("detect scope {:?} in iteration;", batch.tag);
                        }
                        enter.notify_end(end)?;
                    }
                }

                Ok(())
            })
        }
    }

    impl<D: Data> Notifiable for SwitchOperator<D> {
        fn on_end(&mut self, n: End, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
            let level = n.tag().len() as u32;
            if n.port == 0 {
                // the main input;
                if level == 0 {
                    self.exhaust_port = Some(n.take());
                } else if level == self.scope_level - 1 {
                    assert!(self.in_iteration.contains_key(n.tag()));
                    self.parent_scope_ends
                        .insert(n.tag().clone(), vec![EndGuard::new(n.take())]);
                } else {
                    let end = EndGuard::new(n.take());
                    for (t, e) in self.parent_scope_ends.iter_mut() {
                        if end.end.tag.is_parent_of(&*t) {
                            e.push(end.clone());
                        }
                    }
                    if let Some(end) = end.try_unwrap() {
                        outputs[0].notify_end(end)?;
                    }
                }
            } else if n.port == 1 {
                if level == 0 {
                    if let Some(end) = self.exhaust_port.take() {
                        if !self.in_iteration.is_empty() {
                            for (t, _) in self.in_iteration.iter() {
                                error_worker!("can't close iteration as scope {:?} in iteration;", t);
                            }
                            return Err(JobExecError::panic("can't close iteration;".to_owned()));
                        }
                        assert!(self.parent_scope_ends.is_empty());
                        outputs[0].notify_end(end.clone())?;
                    } else {
                        return Err(JobExecError::panic(
                            "inner error : feedback can't exhaust before main input;".to_owned(),
                        ));
                    }
                }
            } else {
                unreachable!("unknown port {}", n.port);
            }
            Ok(())
        }

        fn on_cancel(&mut self, n: Cancel, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
            if n.port == 0 {
                // received from outer loop
                assert!(n.tag().len() < self.scope_level as usize);
                trace_worker!("EARLY_STOP: cancel all iterations of scope {:?};", n.tag());
                inputs[0].cancel_scope(n.tag());
                inputs[1].cancel_scope(n.tag());
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
}
