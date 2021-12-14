use crate::api::notification::{Cancel, End};
use crate::api::IterCondition;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::errors::JobExecError;
use crate::operator::{Notifiable, OperatorCore};
use crate::progress::EndOfScope;
use crate::tag::tools::map::TidyTagMap;
use crate::Data;

struct IterateState {
    iterating: bool,
    src_end: Option<EndOfScope>,
}

impl IterateState {
    fn new() -> Self {
        IterateState { iterating: true, src_end: None }
    }

    fn set_end(&mut self, end: EndOfScope) {
        self.src_end = Some(end);
    }

    fn take_end(&mut self) -> Option<EndOfScope> {
        self.src_end.take()
    }

    fn leave_iteration(&mut self) {
        self.iterating = false;
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
    iterate_states: TidyTagMap<IterateState>,
    parent_parent_scope_ends: Vec<Vec<EndOfScope>>,
    has_synchronized: bool,
}

impl<D> SwitchOperator<D> {
    pub fn new(scope_level: u32, cond: IterCondition<D>) -> Self {
        assert!(scope_level > 0);
        let mut parent_parent_scope_ends = Vec::with_capacity(scope_level as usize + 1);
        for _ in 0..scope_level + 1 {
            parent_parent_scope_ends.push(vec![]);
        }
        SwitchOperator {
            scope_level,
            cond,
            iterate_states: TidyTagMap::new(scope_level - 1),
            parent_parent_scope_ends,
            has_synchronized: false,
        }
    }
}

impl<D: Data> OperatorCore for SwitchOperator<D> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let leave = new_output::<D>(&outputs[0]);
        let enter = new_output::<D>(&outputs[1]);
        let mut main = new_input_session::<D>(&inputs[0]);
        main.for_each_batch(|batch| {
            if !batch.is_empty() {
                let mut leave = leave.new_session(&batch.tag)?;
                let mut enter = enter.new_session(&batch.tag)?;
                for data in batch.drain() {
                    if self.cond.is_converge(&data)? {
                        leave.give(data)?;
                    } else {
                        enter.give(data)?;
                    }
                }
            }

            if let Some(end) = batch.take_end() {
                let p = batch.tag.to_parent_uncheck();
                trace_worker!("detect scope {:?} in iteration;", batch.tag);
                self.iterate_states
                    .insert(p, IterateState::new());
                enter.notify_end(end)?;
            }

            Ok(())
        })?;

        let mut feedback_sync = new_input_session::<D>(&inputs[1]);
        feedback_sync.for_each_batch(|batch| {
            self.has_synchronized = true;
            if batch.tag.current_uncheck() >= self.cond.max_iters {
                let end = batch.take_end();
                if !batch.is_empty() {
                    leave.push_batch_mut(batch)?;
                }
                if let Some(_) = end {
                    trace_worker!("detect {:?} leave iteration;", batch.tag);
                    let p = batch.tag.to_parent_uncheck();
                    if let Some(mut state) = self.iterate_states.remove(&p) {
                        state.leave_iteration();
                        if let Some(end) = state.take_end() {
                            if !end.tag.is_root() {
                                leave.notify_end(end.clone())?;
                            }
                            enter.notify_end(end)?;
                        } else {
                            warn_worker!("{:?} not end while {:?} leave iteration;", p, batch.tag);
                        }
                    } else {
                        error_worker!("iteration for {:?} not found;", p);
                        panic!("iteration for {:?} not found", p);
                    }

                    if self.iterate_states.is_empty() {
                        trace_worker!("detect no scope in iteration;");
                        let len = self.parent_parent_scope_ends.len();
                        for i in (0..len).rev() {
                            for end in self.parent_parent_scope_ends[i].drain(..) {
                                if !end.tag.is_root() {
                                    outputs[0].notify_end(end.clone())?;
                                }
                                outputs[1].notify_end(end)?;
                            }
                        }
                    } else {
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
                    if !self.iterate_states.contains_key(&p) {
                        trace_worker!("detect scope {:?} in iteration;", batch.tag);
                        self.iterate_states
                            .insert(p, IterateState::new());
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
            trace_worker!("iteration: switch on notify end of {:?};", n.tag());
            if level == self.scope_level - 1 {
                if let Some(state) = self.iterate_states.get_mut(n.tag()) {
                    trace_worker!("iteration: switch stash end of {:?}", n.tag());
                    state.set_end(n.take());
                } else {
                    panic!("iteration of {:?} not found;", n.tag())
                }
            } else {
                // parent of parent scope end;
                assert!(level < self.scope_level - 1);
                if self.has_synchronized && self.iterate_states.is_empty() {
                    let end = n.take();
                    if !end.tag.is_root() {
                        outputs[0].notify_end(end.clone())?;
                    }
                    outputs[1].notify_end(end)?;
                } else {
                    self.parent_parent_scope_ends[level as usize].push(n.take());
                }
            }
        } else if n.port == 1 {
            if level == 0 {
                assert!(self.has_synchronized && self.iterate_states.is_empty());
                outputs[0].notify_end(n.take())?;
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
