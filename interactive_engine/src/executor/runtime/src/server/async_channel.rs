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

//! Create two async channel, one is mpsc `async_mpsc`, the other is mpmc `async_mpmc`
//! The two channel can be used as a communication primitive between tasks running on futures executors.
//!
//! `async_mpsc` channel provides `AsyncSSender` and `AsyncSReceiver` handles. `AsyncSReceiver` implements `Future` and
//! allows a task to read values out of the channel. If there is no message to read from the channel,
//! the current task will be notified when a new value is sent by `AsyncSSender`.
//!
//! `async_mpmc` channel provides `AsyncMSender` and `AsyncMReceiver` handles. `AsyncMReceiver` implements `Future` and
//! allows a task to read values out of the channel. If there is no message to read from the channel, the current task
//! will be record to a task queue, and when a new value is sent by `AsyncMSender`, the head task in the task queue will
//! be notified.
//!
//! The difference is `async_mpsc` can be used only one `AsyncSReceiver` is waiting for messages sent by multi `AsyncSSender`,
//! while `async_mpmc` allows multi `AsyncMReceiver` waiting for messages sent by multi `AsyncMSender`.
//!

use futures::task::{Task, current};

use crossbeam_channel::{Sender, Receiver, unbounded, SendError, RecvTimeoutError, TryRecvError };
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Duration;


/// `TryPark` indicate whether task of AsyncSReceiver will be park
///
/// Returned from AsyncSReceiver::try_park()
pub enum TryPark {
    /// Represent AsyncReceiver will park, and it be notified by AsyncSSender after it send a data
    Parked,
    /// AsyncSender has send some data and AsyncReceiver will receive these data
    NotEmpty,
}

/// Futures task, it is shared by `AsyncSSender` and `AsyncSReceiver`
/// `AsyncReceiver` will initialize this task when there is no message in the channel
/// `AsyncSender` will notify this task after it sending data successfully
struct AsyncTask {
    /// Indicate whether current recv task is parked
    unpark: bool,
    /// Record recv task
    task: Option<Task>,
}

/// The transmission end of `async_mpsc` channel which is used to send values
///
/// This is created by `async_mpsc` method
pub struct AsyncSSender<T> {
    sender: Sender<T>,
    task: Arc<Mutex<AsyncTask>>,
}

impl<T> AsyncSSender<T> {
    /// Send a data and signal `AsyncSReceiver` task after sending data successfully
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        match self.sender.send(t) {
            Ok(_) => {
                self.signal();
                Ok(())
            }
            Err(e) => Err(e)
        }
    }

    /// Signal `AsyncSReceiver` task after a message has been sent successfully
    fn signal(&self) {
        let task = {
            let mut recv_task = self.task.lock().expect("Lock self task error. ");
            if recv_task.unpark {
                return;
            }

            recv_task.unpark = true;
            recv_task.task.take()
        };

        if let Some(task) = task {
            task.notify();
        }
    }
}

impl<T> Clone for AsyncSSender<T> {
    fn clone(&self) -> AsyncSSender<T> {
        AsyncSSender {
            sender: self.sender.clone(),
            task: self.task.clone(),
        }
    }
}

/// The transmission end of `async_mpsc` channel which is used to receive values
///
/// This is created by `async_mpsc` method
pub struct AsyncSReceiver<T> {
    receiver: Receiver<T>,
    task: Arc<Mutex<AsyncTask>>,
}

impl<T> Clone for AsyncSReceiver<T> {
    fn clone(&self) -> Self {
        AsyncSReceiver {
            receiver: self.receiver.clone(),
            task: self.task.clone(),
        }
    }
}

impl<T> AsyncSReceiver<T> {
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    /// Try to park the receiver task
    pub fn try_park(&self) -> TryPark {
        let mut recv_task = self.task.lock().expect("recv lock the task error.");
        if recv_task.unpark {
            recv_task.unpark = false;
            return TryPark::NotEmpty;
        }
        recv_task.task = Some(current());
        TryPark::Parked
    }
}

/// Create a async mpsc channel
///
/// this channel can be used only one `AsyncSReceiver` is waiting for messages sent by `AsyncSSender`
pub fn async_mpsc<T>() -> (AsyncSSender<T>, AsyncSReceiver<T>) {
    let async_task = Arc::new(Mutex::new(AsyncTask {
        unpark: false,
        task: None,
    }));

    let (sender, receiver) = unbounded();

    let tx = AsyncSSender {
        sender,
        task: async_task.clone(),
    };

    let rx = AsyncSReceiver {
        receiver,
        task: async_task,
    };

    (tx, rx)
}


#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use crossbeam_channel::TryRecvError;
    use server::async_channel::{async_mpsc, TryPark};
    use futures::future;
    use futures::Future;
    use futures::Async;


    #[test]
    fn async_stream_message() {
        let (sender, receiver) = async_mpsc();
        let mut result = 0;

        let (s, r) = crossbeam_channel::bounded(1);

        let handle = thread::spawn(move || {
            s.send("start").expect("send start failed.");
            sender.send(1).expect("sender 1 failed.");
            thread::sleep(Duration::from_millis(100));
            sender.send(2).expect("sender 2 failed.");
            thread::sleep(Duration::from_millis(100));
            sender.send(3).expect("sender 3 failed.");
            ::std::mem::drop(sender);
        });

        let receive_future = future::poll_fn(|| {
            loop {
                match receiver.try_recv() {
                    // 3: indicate finish
                    Ok(3) => {
                        result = result + 3;
                        return Ok(Async::Ready(result));
                    }
                    //100: indicate error
                    Ok(100) => {
                        return Err(());
                    }
                    Ok(number) => {
                        result = result + number;
                        continue;
                    }
                    Err(TryRecvError::Empty) => {
                        match receiver.try_park() {
                            TryPark::NotEmpty => {
                                continue;
                            }
                            TryPark::Parked => {
                                return Ok(Async::NotReady);
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        return Ok(Async::Ready(result));
                    }
                }
            }
        }).and_then(|result| {
            Ok(result)
        });

        r.recv().expect("waiting start failed.");

        let result = receive_future.wait().expect("execute_async error");
        assert_eq!(result, 6);
        handle.join().unwrap();
    }

    #[test]
    fn async_single_message() {
        let (sender, receiver) = async_mpsc();
        let (s, r) = crossbeam_channel::bounded(1);
        let handle = thread::spawn(move || {
            s.send("start").expect("send start failed.");
            thread::sleep(Duration::from_millis(100));
            sender.send(1).expect("sender 1 failed.");
            sender.send(2).expect("sender 2 failed.");
            ::std::mem::drop(sender);
        });

        let receive_future = future::poll_fn(|| {
            loop {
                match receiver.try_recv() {
                    Ok(number) => {
                        return Ok(Async::Ready(number));
                    }
                    Err(TryRecvError::Empty) => {
                        match receiver.try_park() {
                            TryPark::NotEmpty => {
                                continue;
                            }
                            TryPark::Parked => {
                                return Ok(Async::NotReady);
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        return Err(())
                    }
                }
            }
        }).and_then(|result| {
            Ok(result)
        });

        r.recv().expect("waiting start failed.");

        let result = receive_future.wait().expect("execute_async error");
        assert_eq!(result, 1);
        handle.join().unwrap();
    }
}


