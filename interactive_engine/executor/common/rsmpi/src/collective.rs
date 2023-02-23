//! Collective communication
//!
//! Developing...
//!
//! # Unfinished features
//!
//! - **5.8**: All-to-all, `MPI_Alltoallw()`
//! - **5.10**: Reduce-scatter, `MPI_Reduce_scatter()`
//! - **5.12**: Nonblocking collective operations,
//! `MPI_Ialltoallw()`, `MPI_Ireduce_scatter()`

#[cfg(feature = "user-operations")]
use std::mem;
use std::os::raw::{c_int, c_void};
use std::{fmt, ptr};

#[cfg(feature = "user-operations")]
use libffi::middle::{Cif, Closure, Type};

use crate::ffi;
use crate::ffi::MPI_Op;

use crate::datatype::traits::*;
#[cfg(feature = "user-operations")]
use crate::datatype::{DatatypeRef, DynBuffer, DynBufferMut};
use crate::raw::traits::*;
use crate::request::{Request, Scope, StaticScope};
use crate::topology::traits::*;
use crate::topology::{Process, Rank};
use crate::with_uninitialized;

/// Collective communication traits
pub mod traits {
    pub use super::{CommunicatorCollectives, Operation, Root};
}

/// Collective communication patterns defined on `Communicator`s
pub trait CommunicatorCollectives: Communicator {
    /// Barrier synchronization among all processes in a `Communicator`
    ///
    /// Partake in a barrier synchronization across all processes in the `Communicator` `&self`.
    ///
    /// Calling processes (or threads within the calling processes) will enter the barrier and block
    /// execution until all processes in the `Communicator` `&self` have entered the barrier.
    ///
    /// # Examples
    ///
    /// See `examples/barrier.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.3
    fn barrier(&self) {
        unsafe {
            ffi::MPI_Barrier(self.as_raw());
        }
    }

    /// Gather contents of buffers on all participating processes.
    ///
    /// After the call completes, the contents of the send `Buffer`s on all processes will be
    /// concatenated into the receive `Buffer`s on all ranks.
    ///
    /// All send `Buffer`s must contain the same count of elements.
    ///
    /// # Examples
    ///
    /// See `examples/all_gather.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.7
    fn all_gather_into<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: Buffer,
        R: BufferMut,
    {
        unsafe {
            ffi::MPI_Allgather(
                sendbuf.pointer(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.count() / self.size(),
                recvbuf.as_datatype().as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Gather contents of buffers on all participating processes.
    ///
    /// After the call completes, the contents of the send `Buffer`s on all processes will be
    /// concatenated into the receive `Buffer`s on all ranks.
    ///
    /// The send `Buffer`s may contain different counts of elements on different processes. The
    /// distribution of elements in the receive `Buffer`s is specified via `Partitioned`.
    ///
    /// # Examples
    ///
    /// See `examples/all_gather_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.7
    fn all_gather_varcount_into<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: Buffer,
        R: PartitionedBufferMut,
    {
        unsafe {
            ffi::MPI_Allgatherv(
                sendbuf.pointer(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.counts().as_ptr(),
                recvbuf.displs().as_ptr(),
                recvbuf.as_datatype().as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Distribute the send `Buffer`s from all processes to the receive `Buffer`s on all processes.
    ///
    /// Each process sends and receives the same count of elements to and from each process.
    ///
    /// # Examples
    ///
    /// See `examples/all_to_all.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.8
    fn all_to_all_into<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: Buffer,
        R: BufferMut,
    {
        let c_size = self.size();
        unsafe {
            ffi::MPI_Alltoall(
                sendbuf.pointer(),
                sendbuf.count() / c_size,
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.count() / c_size,
                recvbuf.as_datatype().as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Distribute the send `Buffer`s from all processes to the receive `Buffer`s on all processes.
    ///
    /// The count of elements to send and receive to and from each process can vary and is specified
    /// using `Partitioned`.
    ///
    /// # Standard section(s)
    ///
    /// 5.8
    fn all_to_all_varcount_into<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: PartitionedBuffer,
        R: PartitionedBufferMut,
    {
        unsafe {
            ffi::MPI_Alltoallv(
                sendbuf.pointer(),
                sendbuf.counts().as_ptr(),
                sendbuf.displs().as_ptr(),
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.counts().as_ptr(),
                recvbuf.displs().as_ptr(),
                recvbuf.as_datatype().as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Performs a global reduction under the operation `op` of the input data in `sendbuf` and
    /// stores the result in `recvbuf` on all processes.
    ///
    /// # Examples
    ///
    /// See `examples/reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.9.6
    fn all_reduce_into<S: ?Sized, R: ?Sized, O>(&self, sendbuf: &S, recvbuf: &mut R, op: O)
    where
        S: Buffer,
        R: BufferMut,
        O: Operation,
    {
        unsafe {
            ffi::MPI_Allreduce(
                sendbuf.pointer(),
                recvbuf.pointer_mut(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                op.as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Performs an element-wise global reduction under the operation `op` of the input data in
    /// `sendbuf` and scatters the result into equal sized blocks in the receive buffers on all
    /// processes.
    ///
    /// # Examples
    ///
    /// See `examples/reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.10.1
    fn reduce_scatter_block_into<S: ?Sized, R: ?Sized, O>(
        &self,
        sendbuf: &S,
        recvbuf: &mut R,
        op: O,
    ) where
        S: Buffer,
        R: BufferMut,
        O: Operation,
    {
        assert_eq!(recvbuf.count() * self.size(), sendbuf.count());
        unsafe {
            ffi::MPI_Reduce_scatter_block(
                sendbuf.pointer(),
                recvbuf.pointer_mut(),
                recvbuf.count(),
                sendbuf.as_datatype().as_raw(),
                op.as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Performs a global inclusive prefix reduction of the data in `sendbuf` into `recvbuf` under
    /// operation `op`.
    ///
    /// # Examples
    ///
    /// See `examples/scan.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.11.1
    fn scan_into<S: ?Sized, R: ?Sized, O>(&self, sendbuf: &S, recvbuf: &mut R, op: O)
    where
        S: Buffer,
        R: BufferMut,
        O: Operation,
    {
        unsafe {
            ffi::MPI_Scan(
                sendbuf.pointer(),
                recvbuf.pointer_mut(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                op.as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Performs a global exclusive prefix reduction of the data in `sendbuf` into `recvbuf` under
    /// operation `op`.
    ///
    /// # Examples
    ///
    /// See `examples/scan.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.11.2
    fn exclusive_scan_into<S: ?Sized, R: ?Sized, O>(&self, sendbuf: &S, recvbuf: &mut R, op: O)
    where
        S: Buffer,
        R: BufferMut,
        O: Operation,
    {
        unsafe {
            ffi::MPI_Exscan(
                sendbuf.pointer(),
                recvbuf.pointer_mut(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                op.as_raw(),
                self.as_raw(),
            );
        }
    }

    /// Non-blocking barrier synchronization among all processes in a `Communicator`
    ///
    /// Calling processes (or threads within the calling processes) enter the barrier. Completion
    /// methods on the associated request object will block until all processes have entered.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_barrier.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.1
    fn immediate_barrier(&self) -> Request<'static> {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| ffi::MPI_Ibarrier(self.as_raw(), request)).1,
                StaticScope,
            )
        }
    }

    /// Initiate non-blocking gather of the contents of all `sendbuf`s into all `rcevbuf`s on all
    /// processes in the communicator.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_all_gather.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.5
    fn immediate_all_gather_into<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        unsafe {
            let recvcount = recvbuf.count() / self.size();
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iallgather(
                        sendbuf.pointer(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvcount,
                        recvbuf.as_datatype().as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking gather of the contents of all `sendbuf`s into all `rcevbuf`s on all
    /// processes in the communicator.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_all_gather_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.5
    fn immediate_all_gather_varcount_into<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + PartitionedBufferMut,
        Sc: Scope<'a>,
    {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iallgatherv(
                        sendbuf.pointer(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.counts().as_ptr(),
                        recvbuf.displs().as_ptr(),
                        recvbuf.as_datatype().as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking all-to-all communication.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_all_to_all.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.6
    fn immediate_all_to_all_into<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        let c_size = self.size();
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Ialltoall(
                        sendbuf.pointer(),
                        sendbuf.count() / c_size,
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.count() / c_size,
                        recvbuf.as_datatype().as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking all-to-all communication.
    ///
    /// # Standard section(s)
    ///
    /// 5.12.6
    fn immediate_all_to_all_varcount_into<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + PartitionedBuffer,
        R: 'a + PartitionedBufferMut,
        Sc: Scope<'a>,
    {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Ialltoallv(
                        sendbuf.pointer(),
                        sendbuf.counts().as_ptr(),
                        sendbuf.displs().as_ptr(),
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.counts().as_ptr(),
                        recvbuf.displs().as_ptr(),
                        recvbuf.as_datatype().as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiates a non-blocking global reduction under the operation `op` of the input data in
    /// `sendbuf` and stores the result in `recvbuf` on all processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.8
    fn immediate_all_reduce_into<'a, Sc, S: ?Sized, R: ?Sized, O>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
        op: O,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        O: 'a + Operation,
        Sc: Scope<'a>,
    {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iallreduce(
                        sendbuf.pointer(),
                        recvbuf.pointer_mut(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        op.as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiates a non-blocking element-wise global reduction under the operation `op` of the
    /// input data in `sendbuf` and scatters the result into equal sized blocks in the receive
    /// buffers on all processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.9
    fn immediate_reduce_scatter_block_into<'a, Sc, S: ?Sized, R: ?Sized, O>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
        op: O,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        O: 'a + Operation,
        Sc: Scope<'a>,
    {
        assert_eq!(recvbuf.count() * self.size(), sendbuf.count());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Ireduce_scatter_block(
                        sendbuf.pointer(),
                        recvbuf.pointer_mut(),
                        recvbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        op.as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiates a non-blocking global inclusive prefix reduction of the data in `sendbuf` into
    /// `recvbuf` under operation `op`.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_scan.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.11
    fn immediate_scan_into<'a, Sc, S: ?Sized, R: ?Sized, O>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
        op: O,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        O: 'a + Operation,
        Sc: Scope<'a>,
    {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iscan(
                        sendbuf.pointer(),
                        recvbuf.pointer_mut(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        op.as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiates a non-blocking global exclusive prefix reduction of the data in `sendbuf` into
    /// `recvbuf` under operation `op`.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_scan.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.12
    fn immediate_exclusive_scan_into<'a, Sc, S: ?Sized, R: ?Sized, O>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
        op: O,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        O: 'a + Operation,
        Sc: Scope<'a>,
    {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iexscan(
                        sendbuf.pointer(),
                        recvbuf.pointer_mut(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        op.as_raw(),
                        self.as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }
}

impl<C: Communicator> CommunicatorCollectives for C {}

/// Something that can take the role of 'root' in a collective operation.
///
/// Many collective operations define a 'root' process that takes a special role in the
/// communication. These collective operations are implemented as default methods of this trait.
pub trait Root: AsCommunicator {
    /// Rank of the root process
    fn root_rank(&self) -> Rank;

    /// Broadcast of the contents of a buffer
    ///
    /// After the call completes, the `Buffer` on all processes in the `Communicator` of the `Root`
    /// `&self` will contain what it contains on the `Root`.
    ///
    /// # Examples
    ///
    /// See `examples/broadcast.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.4
    fn broadcast_into<Buf: ?Sized>(&self, buffer: &mut Buf)
    where
        Buf: BufferMut,
    {
        unsafe {
            ffi::MPI_Bcast(
                buffer.pointer_mut(),
                buffer.count(),
                buffer.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Gather contents of buffers on `Root`.
    ///
    /// After the call completes, the contents of the `Buffer`s on all ranks will be
    /// concatenated into the `Buffer` on `Root`.
    ///
    /// All send `Buffer`s must have the same count of elements.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/gather.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.5
    fn gather_into<S: ?Sized>(&self, sendbuf: &S)
    where
        S: Buffer,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Gather(
                sendbuf.pointer(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                ptr::null_mut(),
                0,
                u8::equivalent_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Gather contents of buffers on `Root`.
    ///
    /// After the call completes, the contents of the `Buffer`s on all ranks will be
    /// concatenated into the `Buffer` on `Root`.
    ///
    /// All send `Buffer`s must have the same count of elements.
    ///
    /// This function must be called on the root process.
    ///
    /// # Examples
    ///
    /// See `examples/gather.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.5
    fn gather_into_root<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: Buffer,
        R: BufferMut,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            let recvcount = recvbuf.count() / self.as_communicator().size();
            ffi::MPI_Gather(
                sendbuf.pointer(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvcount,
                recvbuf.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Gather contents of buffers on `Root`.
    ///
    /// After the call completes, the contents of the `Buffer`s on all ranks will be
    /// concatenated into the `Buffer` on `Root`.
    ///
    /// The send `Buffer`s may contain different counts of elements on different processes. The
    /// distribution of elements in the receive `Buffer` is specified via `Partitioned`.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/gather_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.5
    fn gather_varcount_into<S: ?Sized>(&self, sendbuf: &S)
    where
        S: Buffer,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Gatherv(
                sendbuf.pointer(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                ptr::null_mut(),
                ptr::null(),
                ptr::null(),
                u8::equivalent_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Gather contents of buffers on `Root`.
    ///
    /// After the call completes, the contents of the `Buffer`s on all ranks will be
    /// concatenated into the `Buffer` on `Root`.
    ///
    /// The send `Buffer`s may contain different counts of elements on different processes. The
    /// distribution of elements in the receive `Buffer` is specified via `Partitioned`.
    ///
    /// This function must be called on the root process.
    ///
    /// # Examples
    ///
    /// See `examples/gather_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.5
    fn gather_varcount_into_root<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: Buffer,
        R: PartitionedBufferMut,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Gatherv(
                sendbuf.pointer(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.counts().as_ptr(),
                recvbuf.displs().as_ptr(),
                recvbuf.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Scatter contents of a buffer on the root process to all processes.
    ///
    /// After the call completes each participating process will have received a part of the send
    /// `Buffer` on the root process.
    ///
    /// All send `Buffer`s must have the same count of elements.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/scatter.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.6
    fn scatter_into<R: ?Sized>(&self, recvbuf: &mut R)
    where
        R: BufferMut,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Scatter(
                ptr::null(),
                0,
                u8::equivalent_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.count(),
                recvbuf.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Scatter contents of a buffer on the root process to all processes.
    ///
    /// After the call completes each participating process will have received a part of the send
    /// `Buffer` on the root process.
    ///
    /// All send `Buffer`s must have the same count of elements.
    ///
    /// This function must be called on the root process.
    ///
    /// # Examples
    ///
    /// See `examples/scatter.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.6
    fn scatter_into_root<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: Buffer,
        R: BufferMut,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        let sendcount = sendbuf.count() / self.as_communicator().size();
        unsafe {
            ffi::MPI_Scatter(
                sendbuf.pointer(),
                sendcount,
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.count(),
                recvbuf.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Scatter contents of a buffer on the root process to all processes.
    ///
    /// After the call completes each participating process will have received a part of the send
    /// `Buffer` on the root process.
    ///
    /// The send `Buffer` may contain different counts of elements for different processes. The
    /// distribution of elements in the send `Buffer` is specified via `Partitioned`.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/scatter_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.6
    fn scatter_varcount_into<R: ?Sized>(&self, recvbuf: &mut R)
    where
        R: BufferMut,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Scatterv(
                ptr::null(),
                ptr::null(),
                ptr::null(),
                u8::equivalent_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.count(),
                recvbuf.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Scatter contents of a buffer on the root process to all processes.
    ///
    /// After the call completes each participating process will have received a part of the send
    /// `Buffer` on the root process.
    ///
    /// The send `Buffer` may contain different counts of elements for different processes. The
    /// distribution of elements in the send `Buffer` is specified via `Partitioned`.
    ///
    /// This function must be called on the root process.
    ///
    /// # Examples
    ///
    /// See `examples/scatter_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.6
    fn scatter_varcount_into_root<S: ?Sized, R: ?Sized>(&self, sendbuf: &S, recvbuf: &mut R)
    where
        S: PartitionedBuffer,
        R: BufferMut,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Scatterv(
                sendbuf.pointer(),
                sendbuf.counts().as_ptr(),
                sendbuf.displs().as_ptr(),
                sendbuf.as_datatype().as_raw(),
                recvbuf.pointer_mut(),
                recvbuf.count(),
                recvbuf.as_datatype().as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Performs a global reduction under the operation `op` of the input data in `sendbuf` and
    /// stores the result on the `Root` process.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.9.1
    fn reduce_into<S: ?Sized, O>(&self, sendbuf: &S, op: O)
    where
        S: Buffer,
        O: Operation,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Reduce(
                sendbuf.pointer(),
                ptr::null_mut(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                op.as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Performs a global reduction under the operation `op` of the input data in `sendbuf` and
    /// stores the result on the `Root` process.
    ///
    /// This function must be called on the root process.
    ///
    /// # Examples
    ///
    /// See `examples/reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.9.1
    fn reduce_into_root<S: ?Sized, R: ?Sized, O>(&self, sendbuf: &S, recvbuf: &mut R, op: O)
    where
        S: Buffer,
        R: BufferMut,
        O: Operation,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            ffi::MPI_Reduce(
                sendbuf.pointer(),
                recvbuf.pointer_mut(),
                sendbuf.count(),
                sendbuf.as_datatype().as_raw(),
                op.as_raw(),
                self.root_rank(),
                self.as_communicator().as_raw(),
            );
        }
    }

    /// Initiate broadcast of a value from the `Root` process to all other processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_broadcast.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.2
    fn immediate_broadcast_into<'a, Sc, Buf: ?Sized>(
        &self,
        scope: Sc,
        buf: &'a mut Buf,
    ) -> Request<'a, Sc>
    where
        Buf: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Ibcast(
                        buf.pointer_mut(),
                        buf.count(),
                        buf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking gather of the contents of all `sendbuf`s on `Root` `&self`.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_gather.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.3
    fn immediate_gather_into<'a, Sc, S: ?Sized>(&self, scope: Sc, sendbuf: &'a S) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        Sc: Scope<'a>,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Igather(
                        sendbuf.pointer(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        ptr::null_mut(),
                        0,
                        u8::equivalent_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking gather of the contents of all `sendbuf`s on `Root` `&self`.
    ///
    /// This function must be called on the root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_gather.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.3
    fn immediate_gather_into_root<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            let recvcount = recvbuf.count() / self.as_communicator().size();
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Igather(
                        sendbuf.pointer(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvcount,
                        recvbuf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking gather of the contents of all `sendbuf`s on `Root` `&self`.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_gather_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.3
    fn immediate_gather_varcount_into<'a, Sc, S: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        Sc: Scope<'a>,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Igatherv(
                        sendbuf.pointer(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        ptr::null_mut(),
                        ptr::null(),
                        ptr::null(),
                        u8::equivalent_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking gather of the contents of all `sendbuf`s on `Root` `&self`.
    ///
    /// This function must be called on the root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_gather_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.3
    fn immediate_gather_varcount_into_root<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + PartitionedBufferMut,
        Sc: Scope<'a>,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Igatherv(
                        sendbuf.pointer(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.counts().as_ptr(),
                        recvbuf.displs().as_ptr(),
                        recvbuf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking scatter of the contents of `sendbuf` from `Root` `&self`.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_scatter.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.4
    fn immediate_scatter_into<'a, Sc, R: ?Sized>(
        &self,
        scope: Sc,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iscatter(
                        ptr::null(),
                        0,
                        u8::equivalent_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.count(),
                        recvbuf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking scatter of the contents of `sendbuf` from `Root` `&self`.
    ///
    /// This function must be called on the root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_scatter.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.4
    fn immediate_scatter_into_root<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            let sendcount = sendbuf.count() / self.as_communicator().size();
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iscatter(
                        sendbuf.pointer(),
                        sendcount,
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.count(),
                        recvbuf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking scatter of the contents of `sendbuf` from `Root` `&self`.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_scatter_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.4
    fn immediate_scatter_varcount_into<'a, Sc, R: ?Sized>(
        &self,
        scope: Sc,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iscatterv(
                        ptr::null(),
                        ptr::null(),
                        ptr::null(),
                        u8::equivalent_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.count(),
                        recvbuf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiate non-blocking scatter of the contents of `sendbuf` from `Root` `&self`.
    ///
    /// This function must be called on the root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_scatter_varcount.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.4
    fn immediate_scatter_varcount_into_root<'a, Sc, S: ?Sized, R: ?Sized>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
    ) -> Request<'a, Sc>
    where
        S: 'a + PartitionedBuffer,
        R: 'a + BufferMut,
        Sc: Scope<'a>,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Iscatterv(
                        sendbuf.pointer(),
                        sendbuf.counts().as_ptr(),
                        sendbuf.displs().as_ptr(),
                        sendbuf.as_datatype().as_raw(),
                        recvbuf.pointer_mut(),
                        recvbuf.count(),
                        recvbuf.as_datatype().as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiates a non-blacking global reduction under the operation `op` of the input data in
    /// `sendbuf` and stores the result on the `Root` process.
    ///
    /// This function must be called on all non-root processes.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_reduce.rs`
    ///
    /// # Standard section(s)
    ///
    /// 5.12.7
    fn immediate_reduce_into<'a, Sc, S: ?Sized, O>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        op: O,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        O: 'a + Operation,
        Sc: Scope<'a>,
    {
        assert_ne!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Ireduce(
                        sendbuf.pointer(),
                        ptr::null_mut(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        op.as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }

    /// Initiates a non-blocking global reduction under the operation `op` of the input data in
    /// `sendbuf` and stores the result on the `Root` process.
    ///
    /// # Examples
    ///
    /// See `examples/immediate_reduce.rs`
    ///
    /// This function must be called on the root process.
    ///
    /// # Standard section(s)
    ///
    /// 5.12.7
    fn immediate_reduce_into_root<'a, Sc, S: ?Sized, R: ?Sized, O>(
        &self,
        scope: Sc,
        sendbuf: &'a S,
        recvbuf: &'a mut R,
        op: O,
    ) -> Request<'a, Sc>
    where
        S: 'a + Buffer,
        R: 'a + BufferMut,
        O: 'a + Operation,
        Sc: Scope<'a>,
    {
        assert_eq!(self.as_communicator().rank(), self.root_rank());
        unsafe {
            Request::from_raw(
                with_uninitialized(|request| {
                    ffi::MPI_Ireduce(
                        sendbuf.pointer(),
                        recvbuf.pointer_mut(),
                        sendbuf.count(),
                        sendbuf.as_datatype().as_raw(),
                        op.as_raw(),
                        self.root_rank(),
                        self.as_communicator().as_raw(),
                        request,
                    )
                })
                .1,
                scope,
            )
        }
    }
}

impl<'a, C: 'a + Communicator> Root for Process<'a, C> {
    fn root_rank(&self) -> Rank {
        self.rank()
    }
}

/// An operation to be used in a reduction or scan type operation, e.g. `MPI_SUM`
pub trait Operation: AsRaw<Raw = MPI_Op> {
    /// Returns whether the operation is commutative.
    ///
    /// # Standard section(s)
    ///
    /// 5.9.7
    fn is_commutative(&self) -> bool {
        unsafe {
            let mut commute = 0;
            ffi::MPI_Op_commutative(self.as_raw(), &mut commute);
            commute != 0
        }
    }
}
impl<'a, T: 'a + Operation> Operation for &'a T {}

/// A built-in operation like `MPI_SUM`
///
/// # Examples
///
/// See `examples/reduce.rs`
///
/// # Standard section(s)
///
/// 5.9.2
#[derive(Copy, Clone)]
pub struct SystemOperation(MPI_Op);

macro_rules! system_operation_constructors {
    ($($ctor:ident => $val:path),*) => (
        $(pub fn $ctor() -> SystemOperation {
            //! A built-in operation
            SystemOperation(unsafe { $val })
        })*
    )
}

impl SystemOperation {
    system_operation_constructors! {
        max => ffi::RSMPI_MAX,
        min => ffi::RSMPI_MIN,
        sum => ffi::RSMPI_SUM,
        product => ffi::RSMPI_PROD,
        logical_and => ffi::RSMPI_LAND,
        bitwise_and => ffi::RSMPI_BAND,
        logical_or => ffi::RSMPI_LOR,
        bitwise_or => ffi::RSMPI_BOR,
        logical_xor => ffi::RSMPI_LXOR,
        bitwise_xor => ffi::RSMPI_BXOR
    }
}

unsafe impl AsRaw for SystemOperation {
    type Raw = MPI_Op;
    fn as_raw(&self) -> Self::Raw {
        self.0
    }
}

impl Operation for SystemOperation {}

trait Erased {}

impl<T> Erased for T {}

/// A user-defined operation.
///
/// The lifetime `'a` of the operation is limited by the lifetime of the underlying closure.
///
/// For safety reasons, `UserOperation` is in of itself not considered an `Operation`, but a
/// reference of it is.  This limitation may be lifted in the future when `Request` objects can
/// store finalizers.
///
/// **Note:** When a `UserOperation` is passed to a non-blocking API call, it must outlive the
/// completion of the request.  This is normally enforced by the safe API, so this is only a concern
/// if you use the unsafe API.  Do not rely on MPI's internal reference-counting here, because once
/// `UserOperation` is destroyed, the closure object will be deallocated even if the `MPI_Op` handle
/// is still alive due to outstanding references.
///
/// # Examples
///
/// See `examples/reduce.rs` and `examples/immediate_reduce.rs`
#[cfg(feature = "user-operations")]
pub struct UserOperation<'a> {
    op: MPI_Op,
    _anchor: Box<dyn Erased + 'a>, // keeps the internal data alive
}

#[cfg(feature = "user-operations")]
impl<'a> fmt::Debug for UserOperation<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("UserOperation").field(&self.op).finish()
    }
}

#[cfg(feature = "user-operations")]
impl<'a> Drop for UserOperation<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::MPI_Op_free(&mut self.op);
        }
    }
}

#[cfg(feature = "user-operations")]
unsafe impl<'a> AsRaw for UserOperation<'a> {
    type Raw = MPI_Op;
    fn as_raw(&self) -> Self::Raw {
        self.op
    }
}

#[cfg(feature = "user-operations")]
impl<'a, 'b> Operation for &'b UserOperation<'a> {}

#[cfg(feature = "user-operations")]
impl<'a> UserOperation<'a> {
    /// Define an operation using a closure.  The operation must be associative.
    ///
    /// This is a more readable shorthand for the `new` method.  Refer to [`new`](#method.new) for
    /// more information.
    pub fn associative<F>(function: F) -> Self
    where
        F: Fn(DynBuffer, DynBufferMut) + Sync + 'a,
    {
        Self::new(false, function)
    }

    /// Define an operation using a closure.  The operation must be both associative and
    /// commutative.
    ///
    /// This is a more readable shorthand for the `new` method.  Refer to [`new`](#method.new) for
    /// more information.
    pub fn commutative<F>(function: F) -> Self
    where
        F: Fn(DynBuffer, DynBufferMut) + Sync + 'a,
    {
        Self::new(true, function)
    }

    /// Creates an associative and possibly commutative operation using a closure.
    ///
    /// The closure receives two arguments `invec` and `inoutvec` as dynamically typed buffers.  It
    /// shall set `inoutvec` to the value of `f(invec, inoutvec)`, where `f` is a binary associative
    /// operation.
    ///
    /// If the operation is also commutative, setting `commute` to `true` may yield performance
    /// benefits.
    ///
    /// **Note:** If the closure panics, the entire program will abort.
    ///
    /// # Standard section(s)
    ///
    /// 5.9.5
    pub fn new<F>(commute: bool, function: F) -> Self
    where
        F: Fn(DynBuffer, DynBufferMut) + Sync + 'a,
    {
        struct ClosureAnchor<F> {
            rust_closure: F,
            _ffi_closure: Option<Closure<'static>>,
        }

        // must box it to prevent moves
        let mut anchor = Box::new(ClosureAnchor {
            rust_closure: function,
            _ffi_closure: None,
        });

        let args = [
            Type::pointer(), // void *
            Type::pointer(), // void *
            Type::pointer(), // int32_t *
            Type::pointer(), // MPI_Datatype *
        ];
        #[allow(unused_mut)]
        let mut cif = Cif::new(args.iter().cloned(), Type::void());
        // MS-MPI uses "stdcall" calling convention on 32-bit x86
        #[cfg(all(msmpi, target_arch = "x86"))]
        cif.set_abi(libffi::raw::ffi_abi_FFI_STDCALL);

        unsafe extern "C" fn trampoline<'a, F: Fn(DynBuffer, DynBufferMut) + Sync + 'a>(
            cif: &libffi::low::ffi_cif,
            _result: &mut c_void,
            args: *const *const c_void,
            user_function: &F,
        ) {
            debug_assert_eq!(4, cif.nargs);

            let (mut invec, mut inoutvec, len, datatype) = (
                *(*args.offset(0) as *const *mut c_void),
                *(*args.offset(1) as *const *mut c_void),
                *(*args.offset(2) as *const *mut i32),
                *(*args.offset(3) as *const *mut ffi::MPI_Datatype),
            );

            let len = *len;
            let datatype = DatatypeRef::from_raw(*datatype);
            if len == 0 {
                // precautionary measure: ensure pointers are not null
                invec = [].as_mut_ptr();
                inoutvec = [].as_mut_ptr();
            }

            user_function(
                DynBuffer::from_raw(invec, len, datatype),
                DynBufferMut::from_raw(inoutvec, len, datatype),
            )
        }

        let op;
        anchor._ffi_closure = Some(unsafe {
            let ffi_closure = Closure::new(cif, trampoline, &anchor.rust_closure);
            op = with_uninitialized(|op| {
                ffi::MPI_Op_create(Some(*ffi_closure.instantiate_code_ptr()), commute as _, op)
            })
            .1;
            mem::transmute(ffi_closure) // erase the lifetime
        });
        UserOperation {
            op,
            _anchor: anchor,
        }
    }

    /// Creates a `UserOperation` from raw parts.
    ///
    /// Here, `anchor` is an arbitrary object that is stored alongside the `MPI_Op`.
    /// This can be used to attach finalizers to the object.
    ///
    /// # Safety
    /// MPI_Op must not be MPI_OP_NULL
    pub unsafe fn from_raw<T: 'a>(op: MPI_Op, anchor: Box<T>) -> Self {
        Self {
            op,
            _anchor: anchor,
        }
    }
}

/// An unsafe user-defined operation.
///
/// Unsafe user-defined operations are created from pointers to functions that have the unsafe
/// signatures of user functions defined in the MPI C bindings, `UnsafeUserFunction`.
///
/// The recommended way to create user-defined operations is through the safer `UserOperation`
/// type. This type can be used as a work-around in situations where the `libffi` dependency is not
/// available.
pub struct UnsafeUserOperation {
    op: MPI_Op,
}

impl fmt::Debug for UnsafeUserOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("UnsafeUserOperation")
            .field(&self.op)
            .finish()
    }
}

impl Drop for UnsafeUserOperation {
    fn drop(&mut self) {
        unsafe {
            ffi::MPI_Op_free(&mut self.op);
        }
    }
}

unsafe impl AsRaw for UnsafeUserOperation {
    type Raw = MPI_Op;
    fn as_raw(&self) -> Self::Raw {
        self.op
    }
}

impl<'a> Operation for &'a UnsafeUserOperation {}

/// A raw pointer to a function that can be used to define an `UnsafeUserOperation`.
#[cfg(not(all(msmpi, target_arch = "x86")))]
pub type UnsafeUserFunction =
    unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_int, *mut ffi::MPI_Datatype);

/// A raw pointer to a function that can be used to define an `UnsafeUserOperation`.
///
/// MS-MPI uses "stdcall" rather than "C" calling convention. "stdcall" is ignored on x86_64
/// Windows and the default calling convention is used instead.
#[cfg(all(msmpi, target_arch = "x86"))]
pub type UnsafeUserFunction =
    unsafe extern "stdcall" fn(*mut c_void, *mut c_void, *mut c_int, *mut ffi::MPI_Datatype);

impl UnsafeUserOperation {
    /// Define an unsafe operation using a function pointer. The operation must be associative.
    ///
    /// This is a more readable shorthand for the `new` method.  Refer to [`new`](#method.new) for
    /// more information.
    ///
    /// # Safety
    /// The construction of an `UnsafeUserOperation` asserts that `function` is safe to be called
    /// in all reductions that this `UnsafeUserOperation` is used in.
    pub unsafe fn associative(function: UnsafeUserFunction) -> Self {
        Self::new(false, function)
    }

    /// Define an unsafe operation using a function pointer.  The operation must be both associative
    /// and commutative.
    ///
    /// This is a more readable shorthand for the `new` method.  Refer to [`new`](#method.new) for
    /// more information.
    ///
    /// # Safety
    /// The construction of an `UnsafeUserOperation` asserts that `function` is safe to be called
    /// in all reductions that this `UnsafeUserOperation` is used in.
    pub unsafe fn commutative(function: UnsafeUserFunction) -> Self {
        Self::new(true, function)
    }

    /// Creates an associative and possibly commutative unsafe operation using a function pointer.
    ///
    /// The function receives raw `*mut c_void` as `invec` and `inoutvec` and the number of elemnts
    /// of those two vectors as a `*mut c_int` `len`. It shall set `inoutvec`
    /// to the value of `f(invec, inoutvec)`, where `f` is a binary associative operation.
    ///
    /// If the operation is also commutative, setting `commute` to `true` may yield performance
    /// benefits.
    ///
    /// **Note:** The user function is not allowed to panic.
    ///
    /// # Standard section(s)
    ///
    /// 5.9.5
    ///
    /// # Safety
    /// The construction of an `UnsafeUserOperation` asserts that `function` is safe to be called
    /// in all reductions that this `UnsafeUserOperation` is used in.
    pub unsafe fn new(commute: bool, function: UnsafeUserFunction) -> Self {
        UnsafeUserOperation {
            op: with_uninitialized(|op| ffi::MPI_Op_create(Some(function), commute as _, op)).1,
        }
    }
}

/// Perform a local reduction.
///
/// # Examples
///
/// See `examples/reduce.rs`
///
/// # Standard section(s)
///
/// 5.9.7
#[allow(clippy::needless_pass_by_value)]
pub fn reduce_local_into<S: ?Sized, R: ?Sized, O>(inbuf: &S, inoutbuf: &mut R, op: O)
where
    S: Buffer,
    R: BufferMut,
    O: Operation,
{
    unsafe {
        ffi::MPI_Reduce_local(
            inbuf.pointer(),
            inoutbuf.pointer_mut(),
            inbuf.count(),
            inbuf.as_datatype().as_raw(),
            op.as_raw(),
        );
    }
}
