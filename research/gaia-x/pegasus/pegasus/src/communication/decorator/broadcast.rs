use crate::channel_id::ChannelInfo;
use crate::communication::cancel::{CancelHandle, MultiConsCancelPtr};
use crate::communication::decorator::evented::EventEmitPush;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::progress::DynPeers;
use crate::Data;

pub struct BroadcastBatchPush<D: Data> {
    pub ch_info: ChannelInfo,
    pushes: Vec<EventEmitPush<D>>,
    cancel_handle: MultiConsCancelPtr,
}

impl<D: Data> BroadcastBatchPush<D> {
    pub fn new(ch_info: ChannelInfo, pushes: Vec<EventEmitPush<D>>) -> Self {
        let cancel_handle = MultiConsCancelPtr::new(ch_info.scope_level, pushes.len());
        BroadcastBatchPush { ch_info, pushes, cancel_handle }
    }

    pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
        CancelHandle::MC(self.cancel_handle.clone())
    }

    fn push_to(&mut self, target: usize, mut batch: MicroBatch<D>) -> Result<(), IOError> {
        let level = batch.tag.len() as u32;
        if level == self.ch_info.scope_level
            && self
                .cancel_handle
                .is_canceled(&batch.tag, target)
        {
            batch.clear();
            if !batch.is_last() {
                return Ok(());
            }
        }

        if let Some(mut end) = batch.take_end() {
            if end.peers().value() == 1 {
                end.update_peers(DynPeers::all());
                batch.set_end(end);
                self.pushes[target].push(batch)?;
            } else {
                if !batch.is_empty() {
                    self.pushes[target].push(batch)?;
                }
                self.pushes[target].sync_end(end, DynPeers::all())?;
            }
        } else {
            self.pushes[target].push(batch)?;
        }

        Ok(())
    }
}

impl<D: Data> Push<MicroBatch<D>> for BroadcastBatchPush<D> {
    fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
        let level = batch.tag.len() as u32;
        if self.pushes.len() == 1 {
            // corner case;
            if level == self.ch_info.scope_level && self.cancel_handle.is_canceled(&batch.tag, 0) {
                batch.clear();
                if !batch.is_last() {
                    return Ok(());
                }
            }
            self.pushes[0].push(batch)
        } else {
            for i in 1..self.pushes.len() {
                self.push_to(i, batch.share())?;
            }

            self.push_to(0, batch)
        }
    }

    fn flush(&mut self) -> Result<(), IOError> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}
