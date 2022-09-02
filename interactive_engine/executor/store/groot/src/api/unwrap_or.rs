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

#[macro_export]
macro_rules! unwrap_ok_or {
    ($res: expr, $e: pat, $code: expr) => {
        match $res {
            Ok(v) => v,
            Err($e) => $code,
        }
    };
}

#[macro_export]
macro_rules! unwrap_some_or {
    ($res: expr, $code: expr) => {
        match $res {
            Some(v) => v,
            None => $code,
        }
    };
}

#[cfg(test)]
mod test {
    use crate::unwrap_ok_or;
    use crate::{GraphError, GraphResult};

    fn call(res: GraphResult<bool>) -> Option<bool> {
        let res = unwrap_ok_or!(res, _, return None);
        Some(res)
    }
    #[test]
    fn test_unwrap_ok_or() {
        let res = GraphError::invalid_condition("msg".to_owned());
        assert_eq!(None, call(Err(res)));

        assert_eq!(Some(true), call(Ok(true)));
    }
}
