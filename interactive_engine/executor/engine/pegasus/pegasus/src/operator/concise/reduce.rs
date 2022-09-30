use crate::api::function::FnResult;
use crate::api::{Reduce, Unary};
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> Reduce<D> for Stream<D> {
    fn reduce_partition<B, F>(self, builder: B) -> Result<SingleItem<D>, BuildJobError>
    where
        F: FnMut(D, D) -> FnResult<D> + Send + 'static,
        B: Fn() -> F + Send + 'static,
    {
        do_reduce(self, builder)
    }

    fn reduce<B, F>(self, builder: B) -> Result<SingleItem<D>, BuildJobError>
    where
        F: FnMut(D, D) -> FnResult<D> + Send + 'static,
        B: Fn() -> F + Send + Sync + 'static,
    {
        let stream = self.aggregate();
        do_reduce(stream, builder)
    }
}

fn do_reduce<D, B, F>(src: Stream<D>, builder: B) -> Result<SingleItem<D>, BuildJobError>
where
    D: Data,
    F: FnMut(D, D) -> FnResult<D> + Send + 'static,
    B: Fn() -> F + Send + 'static,
{
    let single = src.unary("reduce", |info| {
        let mut table = TidyTagMap::<(D, F)>::new(info.scope_level);
        move |input, output| {
            input.for_each_batch(|dataset| {
                let r = if let Some((mut pre, mut f)) = table.remove(&dataset.tag) {
                    for item in dataset.drain() {
                        pre = f(pre, item)?;
                    }
                    Some((pre, f))
                } else {
                    let mut f = (builder)();
                    let mut iter = dataset.drain();
                    if let Some(mut pre) = iter.next() {
                        for item in iter {
                            pre = f(pre, item)?;
                        }
                        Some((pre, f))
                    } else {
                        None
                    }
                };

                if let Some((r, f)) = r {
                    if let Some(end) = dataset.take_end() {
                        let mut session = output.new_session(&dataset.tag)?;
                        session.give_last(Single(r), end)?;
                    } else {
                        table.insert(dataset.tag.clone(), (r, f));
                    }
                }
                Ok(())
            })
        }
    })?;
    Ok(SingleItem::new(single))
}
