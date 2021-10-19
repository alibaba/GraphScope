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

#![allow(dead_code)]

use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::io::{Error, Read};


#[inline]
pub fn rm<P: AsRef<Path>>(path: P) -> Result<(), String> {
    fs::remove_file(path.as_ref()).map_err(|e| format!("rm {:?} failed, {:?}", path.as_ref(), e))
}

pub fn ls<P: AsRef<Path>>(path: P) -> Result<Vec<String>, String> {
    let mut ret = Vec::new();
    let paths = fs::read_dir(path).map_err(|e| format!("{:?}", e))?;
    for path in paths {
        let path_buf = path.map_err(|e| format!("{:?}", e))?.path();
        let filename = path_buf.to_str().ok_or_else(|| format!("error"))?.to_owned();
        ret.push(filename);
    }
    Ok(ret)
}

#[inline]
pub fn mkdir<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    fs::create_dir_all(path)
}

#[inline]
pub fn rmr<P: AsRef<Path>>(path: P) -> Result<(), String> {
    if exists(path.as_ref()) {
        fs::remove_dir_all(path.as_ref())
            .map_err(|e| format!("rm -r {:?} failed, {:?}", path.as_ref(), e))
    } else {
        Ok(())
    }
}

#[inline]
pub fn file_name(path: &str) -> Result<&str, String> {
    let name = Path::new(path).file_name().ok_or_else(|| format!("{} get filename failed", path))?;
    name.to_str().ok_or_else(|| format!("{:?} to str failed", name))
}

#[inline]
pub fn file_size(path: &str) -> Result<usize, String> {
    fs::metadata(path).map(|meta| {
        meta.len() as usize
    }).map_err(|e| format!("get metadata of {} failed, {:?}", path, e))
}

#[inline]
pub fn touch<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    File::create(path)?;
    Ok(())
}

#[inline]
pub fn exists<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().exists()
}

#[inline]
pub fn is_dir<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().is_dir()
}

pub fn create_path<P: AsRef<Path>>(components: &Vec<P>) -> String {
    let mut path_buf = PathBuf::new();
    for c in components.iter() {
        path_buf.push(c);
    }
    path_buf.to_str().unwrap().to_owned()
}

pub fn path_join<P1: AsRef<Path>, P2: AsRef<Path>>(path1: P1, path2: P2) -> String {
    let mut path_buf = PathBuf::new();
    path_buf.push(path1);
    path_buf.push(path2);
    path_buf.to_str().unwrap().to_owned()
}

#[inline]
pub fn mv<P1: AsRef<Path>, P2: AsRef<Path>>(path1: P1, path2: P2) -> Result<(), Error> {
    ::std::fs::rename(path1, path2)
}

pub fn load_file<P: AsRef<Path>>(path: P) -> Result<String, String> {
    File::open(&path).and_then(|mut f| {
        let mut s = String::new();
        f.read_to_string(&mut s).map(|_| s)
    }).map_err(|e| {
        format!("read file {:?} failed because {:?}", path.as_ref(), e)
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_basic_op() {
        let test_dir = "test_dir";
        assert!(!exists(test_dir));
        mkdir(test_dir).unwrap();
        assert!(exists(test_dir) && is_dir(test_dir));

        let test_path1 = "test_dir/aaa";
        let test_path2 = create_path(&vec![test_dir, "aaa"]);
        assert_eq!(test_path1, test_path2.as_str());

        touch(test_path1).unwrap();
        assert!(exists(test_path1));

        let test_path3 = create_path(&vec![test_dir, "bbb"]);
        touch(test_path3.as_str()).unwrap();
        assert!(exists(test_path3.as_str()));

        let files = ls(test_dir).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains(&test_path2));
        assert!(files.contains(&test_path3));


        rmr(test_dir).unwrap();
        assert!(!exists(test_dir));

    }


}
