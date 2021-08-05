use crate::communication::IOResult;
use crate::event::{Event, Signal};
use crate::graph::Port;
use crate::schedule::state::cancel::CancelPolicy;
use crate::schedule::state::inbound::{InboundStreamState, InputEndNotify};
use crate::Tag;

pub struct OperatorScheduler {
    pub index: usize,
    input_events: Vec<Option<InboundStreamState>>,
    cancel_states: Vec<CancelPolicy>,
    discards: Vec<(Port, Tag)>,
}

impl OperatorScheduler {
    pub fn new(
        index: usize, scope_level: usize, inputs_notify: Vec<Option<Box<dyn InputEndNotify>>>,
        output_ports: Option<&[Vec<(usize, bool, usize)>]>,
    ) -> Self {
        let mut input_events = Vec::with_capacity(inputs_notify.len());
        for (i, notify) in inputs_notify.into_iter().enumerate() {
            if let Some(notify) = notify {
                let port = Port::new(index, i);
                let state = InboundStreamState::new(port, scope_level, notify);
                input_events.push(Some(state));
            } else {
                input_events.push(None);
            }
        }
        let cancel_states = if let Some(output_ports) = output_ports {
            debug_worker!("operator index {}, states length is {}", index, output_ports.len());
            let mut states = Vec::with_capacity(output_ports.len());
            for output_port in output_ports.iter() {
                let mut peers_count = 0;
                for (_, _, peers) in output_port {
                    peers_count += *peers;
                }
                debug_worker!("create of states[{}], peers{}", states.len(), peers_count);
                let state = CancelPolicy::new(scope_level, peers_count);
                states.push(state);
            }
            states
        } else {
            vec![]
        };
        OperatorScheduler { index, input_events, cancel_states, discards: vec![] }
    }

    pub fn accept(&mut self, event: Event) -> IOResult<()> {
        let port = event.target_port;
        let src = event.from_worker;
        let signal = event.take_signal();
        match signal {
            Signal::EndSignal(end) => {
                if let Some(Some(state)) = self.input_events.get_mut(port.port) {
                    trace_worker!("accept end of {:?} from {} on port {:?}", end.tag, src, port);
                    state.on_end(src, end)?;
                } else {
                    warn_worker!("unrecognized event;")
                }
            }
            Signal::CancelSignal(cancel_scope) => {
                if let Some(cancel_state) = self.cancel_states.get_mut(port.port) {
                    debug_worker!(
                        "EARLY-STOP: operator {} accept cancel of {:?} from worker {} on port {:?}",
                        self.index,
                        cancel_scope,
                        src,
                        port
                    );
                    if let Some(t) = cancel_state.on_cancel(src, cancel_scope) {
                        debug_worker!(
                            "EARLY-STOP: operator {} received cancel signals tag {:?} from all workers of port {:?}",
                            self.index,
                            t,
                            port
                        );
                        self.discards.push((port, t));
                    }
                } else {
                    warn_worker!("unrecognized event of port {:?}; form worker {}", port, src)
                }
            }
        }
        Ok(())
    }

    pub fn get_discards(&mut self) -> &mut Vec<(Port, Tag)> {
        &mut self.discards
    }
}
