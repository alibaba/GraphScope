use std::marker::PhantomData;

use crate::api::function::{BatchRouteFunction, FnResult};
use crate::channel_id::ChannelInfo;
use crate::communication::cancel::{CancelHandle, DynSingleConsCancelPtr};
use crate::communication::channel::BatchRoute;
use crate::communication::decorator::evented::EventEmitPush;
use crate::communication::decorator::exchange::ExchangeByBatchPush;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::Data;

struct ScopedAggregate<D: Data>(PhantomData<D>);

impl<D: Data> ScopedAggregate<D> {
    fn new() -> Self {
        ScopedAggregate(std::marker::PhantomData)
    }
}

impl<D: Data> BatchRouteFunction<D> for ScopedAggregate<D> {
    fn route(&self, batch: &MicroBatch<D>) -> FnResult<u64> {
        Ok(batch.tag.current_uncheck() as u64)
    }
}

pub struct AggregateBatchPush<D: Data> {
    push: ExchangeByBatchPush<D>,
}

impl<D: Data> AggregateBatchPush<D> {
    pub fn new(info: ChannelInfo, pushes: Vec<EventEmitPush<D>>) -> Self {
        if info.scope_level == 0 {
            let push = ExchangeByBatchPush::new(info, BatchRoute::AllToOne(0), pushes);
            AggregateBatchPush { push }
        } else {
            let chancel_handle = DynSingleConsCancelPtr::new(info.scope_level, pushes.len());
            let mut push =
                ExchangeByBatchPush::new(info, BatchRoute::Dyn(Box::new(ScopedAggregate::new())), pushes);
            push.update_cancel_handle(CancelHandle::DSC(chancel_handle));
            AggregateBatchPush { push }
        }
    }

    pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
        self.push.get_cancel_handle()
    }
}

impl<D: Data> Push<MicroBatch<D>> for AggregateBatchPush<D> {
    fn push(&mut self, batch: MicroBatch<D>) -> Result<(), IOError> {
        self.push.push(batch)
    }

    fn flush(&mut self) -> Result<(), IOError> {
        self.push.flush()
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.push.close()
    }
}
