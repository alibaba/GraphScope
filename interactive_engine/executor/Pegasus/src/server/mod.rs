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

use std::io;
use tokio::prelude::*;
use futures::sync::mpsc::{unbounded, UnboundedSender};
use super::*;
use crate::serialize::{CPDeserialize, CPSerialize};
use crate::common::{Bytes, BytesSlab};

pub mod service;
pub mod clients;

pub use service::{TaskGenerator, Sink, Service};
pub use clients::client::{ClientService, AsyncClient, Callback};

macro_rules! try_nb {
    ($e:expr) => {
        match $e {
            Ok(t) => t,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(Async::NotReady);
            }
            Err(e) => return Err(e.into()),
        }
    };
}

/// Message header which describes the meta information of message;
/// A cross-network message consist of a header and a body, like [header, body], the header
/// is usually write ahead of the body;
pub trait Header: CPSerialize + CPDeserialize + Sized + Copy + Clone + Debug + 'static {
    /// Indicate how many bytes are needed to encode the header;
    fn length() -> usize;

    /// Length of the message's body required bytes after serialize ;
    fn required(&self) -> usize;

    /// Indicate that this is an "end" signal of the connection;
    fn is_end(&self) -> bool;
}

pub struct BinaryMessage<T: Header> {
    pub header: T,
    body: Option<Bytes>
}

impl<T: Header> BinaryMessage<T> {
    pub fn new(header: T, body: Option<Bytes>) -> Self {
        BinaryMessage {
            header,
            body
        }
    }

    #[inline]
    pub fn take_body(&mut self) -> Option<Bytes> {
        self.body.take()
    }
}

pub struct BinaryReader<T, R> {
    slab: BytesSlab,
    reader: R,
    current: Option<T>,
    active: bool,
}

impl<T: Header, R> BinaryReader<T, R> {
    pub fn new(reader: R) -> Self {
        BinaryReader {
            slab: BytesSlab::new(20),
            reader,
            current: None,
            active: true,
        }
    }
}

impl<R: Read, T: Header> BinaryReader<T, R> {
    pub fn read(&mut self) -> Result<Option<BinaryMessage<T>>, io::Error> {
        // extract ready data;
        while self.active {
            self.slab.ensure_capacity(1);
            match self.reader.read(self.slab.empty()) {
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
                Ok(read) => {
                    if read > 0 {
                        debug!("read {} bytes", read);
                        self.slab.make_valid(read);
                    } else {
                        break
                    }
                }
            }
        }

        if self.current.is_none() {
            if let Some(bytes) = self.slab.try_read(T::length()) {
                self.current.replace(T::read_from(bytes)?);
            } else {
                debug!("no ready header");
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }

        if let Some(header) = self.current.take() {
            debug!("try to read with header {:?}", header);
            if header.required() == 0 {
                if header.is_end() {
                    self.active = false;
                    Ok(None)
                } else {
                    Ok(Some(BinaryMessage::new(header, None)))
                }
            } else {
                self.slab.try_read(header.required())
                    .map(|body| {
                      Some(BinaryMessage::new(header, Some(body)))
                    })
                    .ok_or_else(|| {
                        self.current.replace(header);
                        io::Error::from(io::ErrorKind::WouldBlock)
                    })
            }
        } else {
            // message header is not ready;
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
    }
}

impl<R: AsyncRead, T: Header> Stream for BinaryReader<T, R> {
    type Item = BinaryMessage<T>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let result = try_nb!(self.read());
        Ok(result.into())
    }
}

#[derive(Debug, Copy, Clone, Abomonation, Eq, PartialEq)]
pub struct TaskRequestHeader {
    /// unique 64bit id of task;
    pub task_id: u64,
    /// id of task's group if there are multi-groups;
    pub group_id: u32,
    /// indicate how much workers will be created for this task on one server;
    pub workers: u32,
    /// indicate how much servers will be included to run this task;
    pub processes: u32,
    /// binary length of the task description;
    pub length: u64,
}

pub static DISCONNECTED: TaskRequestHeader = TaskRequestHeader {task_id: 0, group_id: 0, workers: 0, processes: 0, length: 0};
pub static TASK_HEAD_SIZE: usize = 28;

impl CPSerialize for TaskRequestHeader {
    fn serialize_len(&self) -> usize {
        TASK_HEAD_SIZE
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        write.write_u64(self.length)?;
        write.write_u64(self.task_id)?;
        write.write_u32(self.group_id)?;
        write.write_u32(self.workers)?;
        write.write_u32(self.processes)
    }
}

impl CPDeserialize for TaskRequestHeader {
    fn read_from(mut bytes: Bytes) -> Result<Self, io::Error> {
        let length = bytes.read_u64()?;
        let task_id = bytes.read_u64()?;
        let group_id = bytes.read_u32()?;
        let workers = bytes.read_u32()?;
        let processes = bytes.read_u32()?;
        Ok(TaskRequestHeader::new(task_id, group_id, length, workers, processes))
    }
}

impl Header for TaskRequestHeader {
    #[inline]
    fn length() -> usize {
        TASK_HEAD_SIZE
    }

    #[inline]
    fn required(&self) -> usize {
       self.length as usize
    }

    #[inline]
    fn is_end(&self) -> bool {
       *self == DISCONNECTED
    }
}

impl TaskRequestHeader {
    pub fn new(task_id: u64, group_id: u32, length: u64, workers: u32, processes: u32) -> Self {
        TaskRequestHeader { task_id, group_id, length, workers, processes }
    }
}

pub struct TaskRequest<T> {
    pub header: TaskRequestHeader,
    body: T,
}

impl<T> TaskRequest<T> {

    pub fn new(header: TaskRequestHeader, body: T) -> Self {
        TaskRequest {
            header,
            body
        }
    }

    #[inline]
    pub fn header(&self) -> &TaskRequestHeader {
        &self.header
    }

    #[inline]
    pub fn task_id(&self) -> &u64 {
        &self.header.task_id
    }

    #[inline]
    pub fn group_id(&self) -> &u32 {
        &self.header.group_id
    }

    pub fn take_body(self) -> T {
        self.body
    }
}

impl From<BinaryMessage<TaskRequestHeader>> for TaskRequest<Option<Bytes>> {
    fn from(mut msg: BinaryMessage<TaskRequestHeader>) -> Self {
        TaskRequest::new(msg.header, msg.take_body())
    }
}

impl<T: CPSerialize> CPSerialize for TaskRequest<T> {
    fn serialize_len(&self) -> usize {
        self.header.serialize_len() + self.body.serialize_len()
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        self.header.write_to(write)?;
        self.body.write_to(write)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ErrorCode {
    SendRequestError,
    CreateTaskError,
    Unknown,
}

impl ErrorCode {
    pub fn get_code(&self) -> u32 {
        match &self {
            ErrorCode::SendRequestError => 1,
            ErrorCode::CreateTaskError => 2,
            ErrorCode::Unknown => 999,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ResponseType {
    OK,
    Error(ErrorCode)
}

impl ResponseType {
    pub fn get_code(&self) -> u32 {
        match self {
            &ResponseType::OK => 0,
            &ResponseType::Error(ref code) => code.get_code()
        }
    }

    pub fn from_code(code: u32) -> Self {
        match code {
            0 => ResponseType::OK,
            1 => ResponseType::Error(ErrorCode::SendRequestError),
            2 => ResponseType::Error(ErrorCode::CreateTaskError),
            _ => ResponseType::Error(ErrorCode::Unknown)
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct TaskResponseHeader {
    pub res_type: ResponseType,
    pub task_id: u64,
    pub length: u64,
}

pub static RESPONSE_HEAD_SIZE: usize = 20;

impl TaskResponseHeader {
    pub fn new(code: u32, task_id: u64, length: u64) -> Self {
        let res_type = ResponseType::from_code(code);
        TaskResponseHeader {
            res_type,
            task_id,
            length
        }
    }

    pub fn on_error(code: ErrorCode, task_id: u64) -> Self {
        let res_type = ResponseType::Error(code);
        TaskResponseHeader {
            res_type,
            task_id,
            length: 0
        }
    }

    #[inline]
    pub fn is_ok(&self) -> bool {
        self.res_type == ResponseType::OK
    }

    pub fn is_body_empty(&self) -> bool {
        self.length == 0
    }
}

impl CPSerialize for TaskResponseHeader {
    fn serialize_len(&self) -> usize {
        RESPONSE_HEAD_SIZE
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        write.write_u64(self.length)?;
        write.write_u32(self.res_type.get_code())?;
        write.write_u64(self.task_id)
    }
}

impl CPDeserialize for TaskResponseHeader {
    fn read_from(mut bytes: Bytes) -> Result<Self, io::Error> {
        let length = bytes.read_u64()?;
        let code = bytes.read_u32()?;
        let task_id = bytes.read_u64()?;
        Ok(TaskResponseHeader::new(code, task_id, length))
    }
}

impl Header for TaskResponseHeader {
    #[inline]
    fn length() -> usize {
        RESPONSE_HEAD_SIZE
    }

    #[inline]
    fn required(&self) -> usize {
        self.length as usize
    }

    fn is_end(&self) -> bool {
        false
    }
}

pub struct TaskResponse<T> {
    pub header: TaskResponseHeader,
    response: Option<T>
}

impl<T: CPSerialize> TaskResponse<T> {
    pub fn new(task_id: u64, res_type: ResponseType, response: T) -> Self {
        let length = response.serialize_len() as u64;
        let header = TaskResponseHeader {res_type, task_id, length};
        TaskResponse {
            header,
            response: Some(response)
        }
    }

    pub fn ok(task_id: u64, response: T) -> Self {
        Self::new(task_id, ResponseType::OK, response)
    }

    pub fn empty(task_id: u64, res_type: ResponseType) -> Self {
        let header = TaskResponseHeader {res_type, task_id, length: 0};
        TaskResponse {
            header,
            response: None
        }
    }
}

impl<T: CPSerialize> CPSerialize for TaskResponse<T> {
    fn serialize_len(&self) -> usize {
        let body_len = self.response.as_ref()
            .map(|r| r.serialize_len()).unwrap_or(0);
        self.header.serialize_len() + body_len
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        self.header.write_to(write)?;
        if let Some(ref body) = self.response {
            body.write_to(write)?;
        }
        Ok(())
    }
}

pub struct Empty;

impl CPSerialize for Empty {
    fn serialize_len(&self) -> usize {
        0
    }

    fn write_to(&self, _write: &mut BytesSlab) -> Result<(), io::Error> {
        Ok(())
    }
}

pub type EmptyResponse = TaskResponse<Empty>;

#[inline]
pub fn new_empty_response(task_id: u64, res_type: ResponseType) -> EmptyResponse {
    TaskResponse::<Empty>::empty(task_id, res_type)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::operator::sink::BinarySinker;
    use crate::serialize::write_binary;

    struct EchoTask;
    impl TaskGenerator for EchoTask {
        fn create_task(&self, task: TaskRequest<Option<Bytes>>, _runtime: &Pegasus, sink: &mut Sink) -> Result<(), String> {
            let task_id = task.header.task_id;
            let res = task.take_body().unwrap();
            let echo = Echo::read_from(res).unwrap();
            let res = TaskResponse::new(task_id, ResponseType::OK, echo);
            let res = write_binary(&res).unwrap();
            sink.sink(res).map_err(|err| format!("sink error: {:?}", err))?;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct Echo(u64);

    impl CPSerialize for Echo {
        fn serialize_len(&self) -> usize {
            8
        }

        fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
            write.write_u64(self.0)
        }
    }

    impl CPDeserialize for Echo {
        fn read_from(mut bytes: Bytes) -> Result<Self, io::Error> {
            let echo = bytes.read_u64()?;
            Ok(Echo(echo))
        }
    }

    struct EchoCallBack {
        pub expect: u64
    }

    impl EchoCallBack {
        pub fn new(expect: u64) -> EchoCallBack {
            EchoCallBack {
                expect
            }
        }
    }

    impl Callback for EchoCallBack {
        fn accept(&mut self, mut res: BinaryMessage<TaskResponseHeader>) -> bool {
            let body = res.take_body().unwrap();
            let echo = Echo::read_from(body).unwrap();
            info!("get echo back '{:?}'", echo);
            assert_eq!(echo.0, self.expect);
            true
        }
    }

    #[test]
    fn test_echo() {
        try_init_logger().ok();

        let (tx, rx) = crossbeam_channel::unbounded();

        ::std::thread::spawn(move || {
            let runtime = ConfigArgs::singleton(1).build();
            let mut service = Service::new(runtime, EchoTask);
            let addr = vec![service.bind().unwrap()];
            tx.send(addr).unwrap();
            service.start(1);
        });
        let mut client_service = ClientService::new();
        let addr = rx.recv().unwrap();
        client_service.start_service(&addr).unwrap();
        let async_client = client_service.new_async_client();

        async_client.new_task(0, 1, 1, Echo(0), EchoCallBack::new(0)).unwrap();
        async_client.new_task(1, 1, 1, Echo(1), EchoCallBack::new(1)).unwrap();
        async_client.new_task(2, 1, 1, Echo(2), EchoCallBack::new(2)).unwrap();
        async_client.waiting_tasks_done();
    }
}
