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

use std::any::Any;
use std::fmt;
use std::collections::HashSet;
use std::ops::Deref;
use super::*;
use crossbeam_channel::Sender;
use crate::common::Port;
use crate::communication::CommType;

pub mod eventio;
pub mod counted;
pub mod tee;
pub mod output;
pub mod input;
pub mod shuffle;

#[derive(Debug)]
pub enum IOError {
    EOS,
    BrokenPipe(String),
    Cancel(Tag, bool),
    ChannelNotFound(ChannelId),
    CapacityBound,
    SystemIOError(::std::io::Error)
}

impl From<::std::io::Error> for IOError {
    fn from(err: ::std::io::Error) -> Self {
        IOError::SystemIOError(err)
    }
}

pub type IOResult<T> = Result<T, IOError>;

/// Communication channel send-end abstraction;
pub trait Push<T>: Send {

    /// Push message of type `T` into the based channel;
    fn push(&mut self, msg: T) -> IOResult<()>;

    /// Since some channel implementation may make buffer inner, override this method
    /// to flush the buffered message;
    /// For the no-buffer channel implementations, call this method has no sideeffect;
    fn flush(&mut self) -> IOResult<()> {
        Ok(())
    }

    /// Close the channel's push-end;
    fn close(&mut self) -> IOResult<()>;
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    fn push(&mut self, msg: T) -> IOResult<()> {
        (**self).push(msg)
    }

    fn flush(&mut self) -> IOResult<()> {
        (**self).flush()
    }

    fn close(&mut self) -> IOResult<()> {
        (**self).close()
    }
}

/// Communication channel receive end abstraction;
pub trait Pull<T>: Send {

    /// Pull message from the channel; Call to this method won't block;
    /// Return `Ok(Option::empty())` if the channel is empty immediately;
    fn pull(&mut self) -> IOResult<Option<T>>;

    ///
    fn try_pull(&mut self, _timeout: usize) -> IOResult<Option<T>> {
        unimplemented!()
    }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    fn pull(&mut self) -> Result<Option<T>, IOError> {
        (**self).pull()
    }

    fn try_pull(&mut self, timeout: usize) -> Result<Option<T>, IOError> {
        (**self).try_pull(timeout)
    }
}

/// This is extension of `Pull<DataSet<D>>`, which support message stashing ability;
/// By default, all messages are permitted to be pulled from channel, user can change
/// this behavior by set stash flags, messages not needed currently are stashed for later read;
pub trait Stash {

    /// Set the stash flag;
    /// Messages whose tag is contained in the flag will be stashed temporarily,
    fn stash(&mut self, tags: &[Tag]);

    /// Eject some stashed messages;
    fn stash_pop(&mut self, tags: &[Tag]);

    fn get_stashed(&self) -> HashSet<&Tag>;
}

pub trait StashedPull<T>: Stash + Pull<T> { }

impl<D:Data, P: ?Sized + Pull<D> + Stash> StashedPull<D> for P {}

/// An edge in the direct cycle graph, representing data stream channels at runtime;
#[derive(Clone)]
pub struct Edge {
    pub id: ChannelId,
    pub source: Port,
    pub target: Port,
    pub local: bool,
    pub scopes: usize,
    pub mode: CommType,
}

impl Edge {
    pub fn new(id: ChannelId, src: Port,
               dst: Port, local: bool,
               scopes: usize, mode: CommType) -> Self {
        Edge {
            id,
            source: src,
            target: dst,
            local,
            scopes,
            mode,
        }
    }
}

impl fmt::Debug for Edge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}: {:?} -> {:?}]({})", self.id.0, self.source, self.target, self.local)
    }
}


#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataSet<T> {
    tag: Tag,
    data: Vec<T>
}

impl<D: Data> DataSet<D> {

    pub fn new<T: Into<Tag>>(tag: T, data: Vec<D>) -> Self {
        DataSet {
            tag: tag.into(),
            data
        }
    }

    #[inline]
    pub fn take(self) -> (Tag, Vec<D>) {
        (self.tag, self.data)
    }

    #[inline]
    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    #[inline]
    pub fn data(self) -> Vec<D> {
        self.data
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<D: Data> Deref for DataSet<D> {
    type Target = Tag;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tag
    }
}

pub type DataPush<D> = dyn Push<DataSet<D>>;
pub type DataPull<D> = dyn Pull<DataSet<D>>;

