use crate::channel_id::ChannelInfo;
use crate::communication::decorator::evented::ControlPush;
use crate::communication::decorator::ScopeStreamPush;
use crate::communication::IOResult;
use crate::data::DataSet;
use crate::data_plane::GeneralPush;
use crate::event::emitter::EventEmitter;
use crate::graph::Port;
use crate::progress::{EndSignal, Weight};
use crate::{Data, Tag};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct BroadcastBatchPush<D: Data> {
    pub ch_info: ChannelInfo,
    //pool: MemBatchPool<D>,
    pushes: Vec<ControlPush<D>>,
}

impl<D: Data> BroadcastBatchPush<D> {
    pub fn new(
        ch_info: ChannelInfo, has_cycles: Arc<AtomicBool>, pushes: Vec<GeneralPush<DataSet<D>>>,
        event_emitter: &EventEmitter,
    ) -> Self {
        let mut decorated = Vec::with_capacity(pushes.len());
        let source = crate::worker_id::get_current_worker().index;
        for (i, p) in pushes.into_iter().enumerate() {
            let has_cycles = has_cycles.clone();
            let p = ControlPush::new(ch_info, source, i as u32, has_cycles, p, event_emitter.clone());
            decorated.push(p);
        }
        BroadcastBatchPush { ch_info, pushes: decorated }
    }
}

impl<T: Data> ScopeStreamPush<DataSet<T>> for BroadcastBatchPush<T> {
    fn port(&self) -> Port {
        self.ch_info.source_port
    }

    fn push(&mut self, tag: &Tag, msg: DataSet<T>) -> IOResult<()> {
        for i in 1..self.pushes.len() {
            // TODO: avoid clone msg;
            self.pushes[i].push(tag, msg.clone())?;
        }
        self.pushes[0].push(tag, msg)
    }

    fn push_last(&mut self, msg: DataSet<T>, mut end: EndSignal) -> IOResult<()> {
        end.update_to(Weight::all());
        for i in 1..self.pushes.len() {
            // TODO: avoid clone msg;
            self.pushes[i].push_last(msg.clone(), end.clone())?;
        }
        self.pushes[0].push_last(msg, end)
    }

    fn notify_end(&mut self, mut end: EndSignal) -> IOResult<()> {
        end.update_to(Weight::all());
        for i in 1..self.pushes.len() {
            self.pushes[i].notify_end(end.clone())?;
        }
        self.pushes[0].notify_end(end)
    }

    fn flush(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}
