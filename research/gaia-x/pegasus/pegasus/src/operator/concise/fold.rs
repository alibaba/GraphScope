use std::fmt::Debug;

use crate::api::function::FnResult;
use crate::api::{Fold, Unary};
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> Fold<D> for Stream<D> {
    fn fold_partition<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static,
    {
        let s = self.unary("fold_partition", |info| {
            let mut table = TidyTagMap::<(B, F)>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let (mut accum, mut f) = table
                        .remove(&dataset.tag)
                        .unwrap_or((init.clone(), factory()));

                    for d in dataset.drain() {
                        accum = f(accum, d)?;
                    }

                    if dataset.is_last() {
                        let mut session = output.new_session(&dataset.tag)?;
                        let end = dataset.take_end().expect("unreachable");
                        session.give_last(Single(accum), end)?;
                    } else {
                        table.insert(dataset.tag.clone(), (accum, f));
                    }
                    Ok(())
                })
            }
        })?;
        Ok(SingleItem::new(s))
    }

    fn fold<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static,
    {
        let s = self.aggregate().unary("fold", |info| {
            let mut table = TidyTagMap::<(B, F)>::new(info.scope_level);
            let worker_id = crate::worker_id::get_current_worker();
            let index = worker_id.index;
            let peers = worker_id.total_peers();
            move |input, output| {
                input.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let (mut accum, mut f) = table
                            .remove(&dataset.tag)
                            .unwrap_or((init.clone(), factory()));

                        for d in dataset.drain() {
                            accum = f(accum, d)?;
                        }

                        if dataset.is_last() {
                            let mut session = output.new_session(&dataset.tag)?;
                            let end = dataset.take_end().expect("unreachable");
                            trace_worker!("fold all data and emit result of {:?} ;", dataset.tag);
                            session.give_last(Single(accum), end)?;
                        } else {
                            table.insert(dataset.tag.clone(), (accum, f));
                        }
                    } else if dataset.is_last() {
                        // dataset empty and is last;
                        let mut session = output.new_session(&dataset.tag)?;
                        let end = dataset.take_end().expect("unreachable");
                        let (accum, _) = table
                            .remove(&dataset.tag)
                            .unwrap_or((init.clone(), factory()));
                        let flag = if dataset.tag.len() == 0 {
                            index == 0
                        } else {
                            dataset.tag.current_uncheck() % peers == index
                        };
                        if flag {
                            trace_worker!("fold all data and emit result of {:?} ;", dataset.tag);
                            session.give_last(Single(accum), end)?;
                        } else {
                            session.notify_end(end)?;
                        }
                    }
                    Ok(())
                })
            }
        })?;
        Ok(SingleItem::new(s))
    }
}
