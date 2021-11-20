use std::fmt::Debug;

use crate::api::function::FnResult;
use crate::api::{Fold, Unary};
use crate::communication::output::OutputProxy;
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> Fold<D> for Stream<D> {
    fn fold<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static,
    {
        let s = self.aggregate().unary("fold", |info| {
            let mut table = TidyTagMap::<(B, F)>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let (mut accum, mut f) = table
                            .remove(&batch.tag)
                            .unwrap_or((init.clone(), factory()));

                        for d in batch.drain() {
                            accum = f(accum, d)?;
                        }

                        if let Some(end) = batch.take_end() {
                            let mut session = output.new_session(&batch.tag)?;
                            trace_worker!("fold all data and emit result of {:?} ;", batch.tag);
                            session.give_last(Single(accum), end)?;
                        } else {
                            table.insert(batch.tag.clone(), (accum, f));
                        }
                        return Ok(());
                    }

                    if let Some(end) = batch.take_end() {
                        if let Some((accum, _)) = table.remove(&batch.tag) {
                            let mut session = output.new_session(&batch.tag)?;
                            session.give_last(Single(accum), end)?;
                        } else {
                            let worker = crate::worker_id::get_current_worker().index;
                            if end.contains_source(worker) {
                                let mut session = output.new_session(&batch.tag)?;
                                session.give_last(Single(init.clone()), end)?
                            } else {
                                output.notify_end(end)?;
                            }
                        }
                    }
                    Ok(())
                })
            }
        })?;
        Ok(SingleItem::new(s))
    }

    fn fold_partition<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static,
    {
        let s = self.unary("fold_partition", |info| {
            let mut table = TidyTagMap::<(B, F)>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let (mut accum, mut f) = table
                            .remove(&batch.tag)
                            .unwrap_or((init.clone(), factory()));

                        for d in batch.drain() {
                            accum = f(accum, d)?;
                        }
                        table.insert(batch.tag.clone(), (accum, f));
                    }

                    if let Some(end) = batch.take_end() {
                        if let Some((accum, _)) = table.remove(&batch.tag) {
                            let mut session = output.new_session(&batch.tag)?;
                            session.give_last(Single(accum), end)?;
                        } else {
                            let worker = crate::worker_id::get_current_worker().index;
                            if end.contains_source(worker) {
                                let mut session = output.new_session(&batch.tag)?;
                                session.give_last(Single(init.clone()), end)?
                            } else {
                                output.notify_end(end)?;
                            }
                        }
                    }
                    Ok(())
                })
            }
        })?;
        Ok(SingleItem::new(s))
    }
}
