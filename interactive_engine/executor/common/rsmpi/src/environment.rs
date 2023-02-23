//! Environmental management
//!
//! This module provides ways for an MPI program to interact with its environment.
//!
//! # Unfinished features
//!
//! - **8.1.2**: `MPI_TAG_UB`, ...
//! - **8.2**: Memory allocation
//! - **8.3, 8.4, and 8.5**: Error handling

use std::{
    cmp::Ordering,
    os::raw::{c_char, c_double, c_int, c_void},
    ptr,
    string::FromUtf8Error,
    sync::RwLock,
    thread::{self, ThreadId},
};

use conv::ConvUtil;
use once_cell::sync::Lazy;

use crate::ffi;
use crate::topology::SystemCommunicator;
use crate::{with_uninitialized, with_uninitialized2};

/// Internal data structure used to uphold certain MPI invariants.
/// State is currently only used with the derive feature.
pub(crate) struct UniverseState {
    #[allow(unused)]
    pub main_thread: ThreadId,
}

pub(crate) static UNIVERSE_STATE: Lazy<RwLock<Option<UniverseState>>> =
    Lazy::new(|| RwLock::new(None));

/// Global context
pub struct Universe {
    buffer: Option<Vec<u8>>,
}

impl Universe {
    /// The 'world communicator'
    ///
    /// Contains all processes initially partaking in the computation.
    ///
    /// # Examples
    /// See `examples/simple.rs`
    pub fn world(&self) -> SystemCommunicator {
        SystemCommunicator::world()
    }

    /// The size in bytes of the buffer used for buffered communication.
    pub fn buffer_size(&self) -> usize {
        self.buffer.as_ref().map_or(0, Vec::len)
    }

    /// Set the size in bytes of the buffer used for buffered communication.
    pub fn set_buffer_size(&mut self, size: usize) {
        self.detach_buffer();

        if size > 0 {
            let mut buffer = vec![0; size];
            unsafe {
                ffi::MPI_Buffer_attach(
                    buffer.as_mut_ptr() as _,
                    buffer
                        .len()
                        .value_as()
                        .expect("Buffer length exceeds the range of a C int."),
                );
            }
            self.buffer = Some(buffer);
        }
    }

    /// Detach the buffer used for buffered communication.
    pub fn detach_buffer(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let mut addr: *const c_void = ptr::null();
            let addr_ptr: *mut *const c_void = &mut addr;
            let mut size: c_int = 0;
            unsafe {
                ffi::MPI_Buffer_detach(addr_ptr as *mut c_void, &mut size);
                assert_eq!(addr, buffer.as_ptr() as _);
            }
            assert_eq!(
                size,
                buffer
                    .len()
                    .value_as()
                    .expect("Buffer length exceeds the range of a C int.")
            );
        }
    }
}

impl Drop for Universe {
    fn drop(&mut self) {
        // This can only ever be called once since it's only possible to initialize a single
        // Universe per application run.
        //
        // NOTE: The write lock is taken to prevent racing with `#[derive(Equivalence)]`
        let mut _universe_state = UNIVERSE_STATE
            .write()
            .expect("rsmpi internal error: UNIVERSE_STATE lock poisoned");

        self.detach_buffer();
        unsafe {
            ffi::MPI_Finalize();
        }
    }
}

/// Describes the various levels of multithreading that can be supported by an MPI library.
///
/// # Examples
/// See `examples/init_with_threading.rs`
///
/// # Standard section(s)
///
/// 12.4.3
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Threading {
    /// All processes partaking in the computation are single-threaded.
    Single,
    /// Processes may be multi-threaded, but MPI functions will only ever be called from the main
    /// thread.
    Funneled,
    /// Processes may be multi-threaded, but calls to MPI functions will not be made concurrently.
    /// The user is responsible for serializing the calls.
    Serialized,
    /// Processes may be multi-threaded with no restrictions on the use of MPI functions from the
    /// threads.
    Multiple,
}

impl Threading {
    /// The raw value understood by the MPI C API
    fn as_raw(self) -> c_int {
        match self {
            Threading::Single => unsafe { ffi::RSMPI_THREAD_SINGLE },
            Threading::Funneled => unsafe { ffi::RSMPI_THREAD_FUNNELED },
            Threading::Serialized => unsafe { ffi::RSMPI_THREAD_SERIALIZED },
            Threading::Multiple => unsafe { ffi::RSMPI_THREAD_MULTIPLE },
        }
    }
}

impl PartialOrd<Threading> for Threading {
    fn partial_cmp(&self, other: &Threading) -> Option<Ordering> {
        self.as_raw().partial_cmp(&other.as_raw())
    }
}

impl Ord for Threading {
    fn cmp(&self, other: &Threading) -> Ordering {
        self.as_raw().cmp(&other.as_raw())
    }
}

impl From<c_int> for Threading {
    fn from(i: c_int) -> Threading {
        if i == unsafe { ffi::RSMPI_THREAD_SINGLE } {
            return Threading::Single;
        } else if i == unsafe { ffi::RSMPI_THREAD_FUNNELED } {
            return Threading::Funneled;
        } else if i == unsafe { ffi::RSMPI_THREAD_SERIALIZED } {
            return Threading::Serialized;
        } else if i == unsafe { ffi::RSMPI_THREAD_MULTIPLE } {
            return Threading::Multiple;
        }
        panic!("Unknown threading level: {}", i)
    }
}

/// Whether the MPI library has been initialized
pub(crate) fn is_initialized() -> bool {
    unsafe { with_uninitialized(|initialized| ffi::MPI_Initialized(initialized)).1 != 0 }
}

/// Whether the MPI library has been initialized
/// NOTE: Used by "derive" feature
#[allow(unused)]
pub(crate) fn is_finalized() -> bool {
    unsafe { with_uninitialized(|finalized| ffi::MPI_Finalized(finalized)).1 != 0 }
}

/// Initialize MPI.
///
/// If the MPI library has not been initialized so far, initializes and returns a representation
/// of the MPI communication `Universe` which provides access to additional functions.
/// Otherwise returns `None`.
///
/// Equivalent to: `initialize_with_threading(Threading::Single)`
///
/// # Examples
/// See `examples/simple.rs`
///
/// # Standard section(s)
///
/// 8.7
pub fn initialize() -> Option<Universe> {
    initialize_with_threading(Threading::Single).map(|x| x.0)
}

/// Initialize MPI with desired level of multithreading support.
///
/// If the MPI library has not been initialized so far, tries to initialize with the desired level
/// of multithreading support and returns the MPI communication `Universe` with access to
/// additional functions as well as the level of multithreading actually supported by the
/// implementation. Otherwise returns `None`.
///
/// # Examples
/// See `examples/init_with_threading.rs`
///
/// # Standard section(s)
///
/// 12.4.3
pub fn initialize_with_threading(threading: Threading) -> Option<(Universe, Threading)> {
    // Takes the lock before checking if MPI is initialized to prevent a race condition
    // leading to two threads both calling `MPI_Init_thread` at the same time.
    //
    // NOTE: This is necessary even without the derive feature - we use this `Mutex` to ensure
    // no race in initializing MPI.
    let mut universe_state = UNIVERSE_STATE
        .write()
        .expect("rsmpi internal error: UNIVERSE_STATE lock poisoned");

    if is_initialized() {
        return None;
    }

    let (_, provided) = unsafe {
        with_uninitialized(|provided| {
            ffi::MPI_Init_thread(
                ptr::null_mut(),
                ptr::null_mut(),
                threading.as_raw(),
                provided,
            )
        })
    };

    // No need to check if UNIVERSE_STATE has already been set - only one thread can enter this
    // code section per MPI run thanks to the `is_initialized()` check before.
    *universe_state = Some(UniverseState {
        main_thread: thread::current().id(),
    });

    Some((Universe { buffer: None }, provided.into()))
}

/// Level of multithreading supported by this MPI universe
///
/// See the `Threading` enum.
///
/// # Examples
/// See `examples/init_with_threading.rs`
pub fn threading_support() -> Threading {
    unsafe {
        with_uninitialized(|threading| ffi::MPI_Query_thread(threading))
            .1
            .into()
    }
}

/// Identifies the version of the MPI standard implemented by the library.
///
/// Returns a tuple of `(version, subversion)`, e.g. `(3, 0)`.
///
/// Can be called without initializing MPI.
pub fn version() -> (c_int, c_int) {
    let (_, version, subversion) = unsafe {
        with_uninitialized2(|version, subversion| ffi::MPI_Get_version(version, subversion))
    };
    (version, subversion)
}

/// Describes the version of the MPI library itself.
///
/// Can return an `Err` if the description of the MPI library is not a UTF-8 string.
///
/// Can be called without initializing MPI.
pub fn library_version() -> Result<String, FromUtf8Error> {
    let bufsize = unsafe { ffi::RSMPI_MAX_LIBRARY_VERSION_STRING }
        .value_as()
        .unwrap_or_else(|_| {
            panic!(
                "MPI_MAX_LIBRARY_SIZE ({}) cannot be expressed as a usize.",
                unsafe { ffi::RSMPI_MAX_LIBRARY_VERSION_STRING }
            )
        });
    let mut buf = vec![0u8; bufsize];
    let mut len: c_int = 0;

    unsafe {
        ffi::MPI_Get_library_version(buf.as_mut_ptr() as *mut c_char, &mut len);
    }
    buf.truncate(len.value_as().unwrap_or_else(|_| {
        panic!(
            "Length of library version string ({}) cannot \
             be expressed as a usize.",
            len
        )
    }));
    String::from_utf8(buf)
}

/// Names the processor that the calling process is running on.
///
/// Can return an `Err` if the processor name is not a UTF-8 string.
pub fn processor_name() -> Result<String, FromUtf8Error> {
    let bufsize = unsafe { ffi::RSMPI_MAX_PROCESSOR_NAME }
        .value_as()
        .unwrap_or_else(|_| {
            panic!(
                "MPI_MAX_LIBRARY_SIZE ({}) \
                 cannot be expressed as a \
                 usize.",
                unsafe { ffi::RSMPI_MAX_PROCESSOR_NAME }
            )
        });
    let mut buf = vec![0u8; bufsize];
    let mut len: c_int = 0;

    unsafe {
        ffi::MPI_Get_processor_name(buf.as_mut_ptr() as *mut c_char, &mut len);
    }
    buf.truncate(len.value_as().unwrap_or_else(|_| {
        panic!(
            "Length of processor name string ({}) cannot be \
             expressed as a usize.",
            len
        )
    }));
    String::from_utf8(buf)
}

/// Time in seconds since an arbitrary time in the past.
///
/// The cheapest high-resolution timer available will be used.
pub fn time() -> c_double {
    unsafe { ffi::RSMPI_Wtime() }
}

/// Resolution of timer used in `time()` in seconds
pub fn time_resolution() -> c_double {
    unsafe { ffi::RSMPI_Wtick() }
}
