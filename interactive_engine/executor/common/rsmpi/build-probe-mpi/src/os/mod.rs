#[cfg(windows)]
mod windows;

#[cfg(unix)]
mod unix;

#[cfg(windows)]
pub use self::windows::probe;

#[cfg(unix)]
pub use self::unix::probe;
