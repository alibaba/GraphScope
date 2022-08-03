use ahash::AHashSet;

use crate::api::{Dedup, HasKey, PartitionByKey, Unary};
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data + HasKey> Dedup<D> for Stream<D> {
    fn dedup(self) -> Result<Stream<D>, BuildJobError> {
        self.partition_by_key().unary("dedup", |info| {
            let mut table = TidyTagMap::<AHashSet<D::Target>>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        let set = table.get_mut_or_insert(&batch.tag);
                        for d in batch.drain() {
                            if !set.contains(d.get_key()) {
                                set.insert(d.get_key().clone());
                                session.give(d)?;
                            }
                        }
                    }

                    if batch.is_last() {
                        table.remove(&batch.tag);
                    }

                    Ok(())
                })
            }
        })
    }
}
