//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::marker::PhantomData;
use super::*;

pub struct ExchangePusher<D, P, H> {
    pushers: Vec<P>,
    buffs: Vec<Vec<D>>,
    current: Option<Tag>,
    hash_func: H,
    batch_size: usize
}

impl<D: Data, P: Push<DataSet<D>>, H: FnMut(&D) -> u64> ExchangePusher<D, P, H> {
    pub fn new(pushers: Vec<P>, hash_func: H, batch_size: usize) -> Self {
        let mut buffs = Vec::with_capacity(pushers.len());
        for _ in 0..pushers.len() {
            buffs.push(Vec::with_capacity(batch_size));
        }

        ExchangePusher {
            pushers,
            buffs,
            current: None,
            hash_func,
            batch_size
        }
    }

    #[inline]
    pub fn flush_one(&mut self, index: usize) -> Result<(), IOError> {
        if self.buffs[index].len() > 0 {
            if let Some(ref tag) = self.current {
                let buff = ::std::mem::replace(&mut self.buffs[index],
                                               Vec::with_capacity(self.batch_size));
                self.pushers[index].push(DataSet::new(tag.clone(), buff))?;
            }
        }
        Ok(())
    }
}

impl<D: Data, P: Push<DataSet<D>>, H: FnMut(&D) -> u64 + Send> Push<DataSet<D>> for ExchangePusher<D, P, H> {
    fn push(&mut self, data: DataSet<D>) -> IOResult<()> {
        if !data.is_empty() {
            if self.pushers.len() == 1 {
                self.pushers[0].push(data)?;
            } else {
                if self.current.as_ref() != Some(data.tag()) {
                    self.flush()?;
                    self.current.replace(data.tag.clone());
                }

                let (_, mut elements) = data.take();
                if (self.pushers.len() & (self.pushers.len() - 1)) == 0 {
                    let mask = (self.pushers.len() - 1) as u64;
                    for datum in elements.drain(..) {
                        let index = (((self.hash_func)(&datum)) & mask) as usize;
                        self.buffs[index].push(datum);
                        if self.buffs[index].len() == self.batch_size {
                            self.flush_one(index)?;
                        }
                    }
                } else {
                    for datum in elements.drain(..) {
                        let index = (((self.hash_func)(&datum)) % self.pushers.len() as u64) as usize;
                        self.buffs[index].push(datum);
                        if self.buffs[index].len() == self.batch_size {
                            self.flush_one(index)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        for i in 0..self.pushers.len() {
            self.flush_one(i)?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        for i in 0..self.pushers.len() {
            self.flush_one(i)?;
            self.pushers[i].close()?;
        }
        Ok(())
    }
}

pub struct BroadcastPusher<D, P> {
    pushers: Vec<P>,
    _ph: PhantomData<D>,
}

impl<D: Data, P> BroadcastPusher<D, P> {
    pub fn new(pushes: Vec<P>) -> Self {
        BroadcastPusher {
            pushers: pushes,
            _ph: PhantomData
        }
    }
}

impl<D: Data, P: Push<DataSet<D>>> Push<DataSet<D>> for BroadcastPusher<D, P> {

    fn push(&mut self, msg: DataSet<D>) -> Result<(), IOError> {
        if !msg.is_empty() {
            for i in 1..self.pushers.len() {
                self.pushers[i].push(msg.clone())?;
            }
            self.pushers[0].push(msg)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), IOError> {
        for push in self.pushers.iter_mut() {
            push.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        for push in self.pushers.iter_mut() {
            push.close()?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_queue::SegQueue;
    use crate::allocate::ThreadPush;

    #[test]
    fn test_exchange_push() {
        let q1 = Arc::new(SegQueue::new());
        let q2 = Arc::new(SegQueue::new());
        let q3 = Arc::new(SegQueue::new());
        let mut pushes = Vec::new();
        pushes.push(ThreadPush::new(q1.clone()));
        pushes.push(ThreadPush::new(q2.clone()));
        pushes.push(ThreadPush::new(q3.clone()));
        let hash_func = |item: &u64| *item;
        let mut ex_push = ExchangePusher::new(pushes, hash_func, 64);
        for i in 0..1000u64 {
           ex_push.push(DataSet::new(0, vec![i;32])).expect("push failure");
        }

        ex_push.close().expect("close failure");

        let mut count = 0;
        while let Ok(dataset) = q1.pop() {
            count += dataset.len();
            for i in dataset.data() {
                assert_eq!(i % 3, 0);
            }
        };
        assert_eq!(count, 334 * 32);

        let mut count = 0;
        while let Ok(dataset) = q2.pop() {
            count += dataset.len();
            for i in dataset.data() {
                assert_eq!(i % 3, 1);
            }
        };
        assert_eq!(count, 333 * 32);

        let mut count = 0;
        while let Ok(dataset) = q3.pop() {
            count += dataset.len();
            for i in dataset.data() {
                assert_eq!(i % 3, 2);
            }
        };
        assert_eq!(count, 333 * 32);
    }
}
