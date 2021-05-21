//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::path::Path;
use std::process::{Command, ExitStatus};

/// Use `std::process::Command` to upload a `local_path` to HDFS as `hdfs_path`
pub fn upload_to_hdfs<P: AsRef<Path>>(
    hdfs_bin: &str, hdfs_path: &str, local_path: P,
) -> std::io::Result<ExitStatus> {
    let upload_str = format!("{} dfs -put {:?} {}", hdfs_bin, local_path.as_ref(), hdfs_path);

    let mut upload_cmd = Command::new("sh");
    upload_cmd.arg("-c").arg(&upload_str);

    upload_cmd.status()
}

/// Use `std::process::Command` to download `hdfs_path` from HDFS to `local_path`
pub fn download_from_hdfs<P: AsRef<Path>>(
    hdfs_bin: &str, hdfs_path: &str, local_path: P,
) -> std::io::Result<ExitStatus> {
    let download_str = format!("{} dfs -get {} {:?}", hdfs_bin, hdfs_path, local_path.as_ref());

    let mut download_cmd = Command::new("sh");
    download_cmd.arg("-c").arg(&download_str);

    download_cmd.status()
}

pub struct Iter<'a, T> {
    inner: Box<dyn Iterator<Item = T> + 'a + Send>,
}

impl<'a, T> Iter<'a, T> {
    pub fn new(iter: Box<dyn Iterator<Item = T> + 'a + Send>) -> Self {
        Iter { inner: iter }
    }

    pub fn from_iter<I: Iterator<Item = T> + 'a + Send>(iter: I) -> Self {
        Iter { inner: Box::new(iter) }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.inner.count()
    }
}

//unsafe impl<'a, T:'a> Send for Iter<'a, T> {}

pub struct IterList<T, I>
where
    T: Iterator<Item = I>,
{
    iters: Vec<T>,
    curr_iter: Option<T>,
}

unsafe impl<T, I> Send for IterList<T, I> where T: Iterator<Item = I> {}

impl<T, I> IterList<T, I>
where
    T: Iterator<Item = I>,
{
    pub fn new(iters: Vec<T>) -> Self {
        IterList { iters, curr_iter: None }
    }
}

impl<T, I> Iterator for IterList<T, I>
where
    T: Iterator<Item = I>,
{
    type Item = I;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        loop {
            if let Some(ref mut iter) = self.curr_iter {
                if let Some(x) = iter.next() {
                    return Some(x);
                } else {
                    if let Some(iter_val) = self.iters.pop() {
                        *iter = iter_val;
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(iter_val) = self.iters.pop() {
                    self.curr_iter = Some(iter_val);
                } else {
                    return None;
                }
            }
        }
    }
}
