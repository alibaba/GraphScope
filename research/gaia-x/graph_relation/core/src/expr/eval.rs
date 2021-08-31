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
//!

use crate::expr::error::{ExprError, ExprResult};
use crate::generated::common as pb;
use crate::generated::common::expr_unit::Item;
use crate::generated::common::{Arithmetic, ExprUnit, Logical};
use dyn_type::arith::Exp;
use dyn_type::{BorrowObject, Object, Primitives};
use std::cell::RefCell;

pub struct Evaluator<'a> {
    suffix_tree: Vec<pb::ExprUnit>,
    stack: RefCell<Vec<BorrowObject<'a>>>,
    // todo shall take a concept to accept values for variable
}

impl<'a> From<Vec<pb::ExprUnit>> for Evaluator<'a> {
    fn from(suffix_tree: Vec<ExprUnit>) -> Self {
        Self {
            suffix_tree,
            stack: RefCell::new(vec![]),
        }
    }
}

fn apply_arith<'a>(
    arith: pb::Arithmetic,
    first: Option<BorrowObject<'a>>,
    second: Option<BorrowObject<'a>>,
) -> ExprResult<BorrowObject<'a>> {
    if first.is_some() && second.is_some() {
        let a = first.unwrap();
        let b = second.unwrap();
        let rst = match arith {
            Arithmetic::Add => BorrowObject::Primitive(a.as_primitive()? + b.as_primitive()?),
            Arithmetic::Sub => BorrowObject::Primitive(a.as_primitive()? - b.as_primitive()?),
            Arithmetic::Mul => BorrowObject::Primitive(a.as_primitive()? * b.as_primitive()?),
            Arithmetic::Div => BorrowObject::Primitive(a.as_primitive()? / b.as_primitive()?),
            Arithmetic::Mod => BorrowObject::Primitive(a.as_primitive()? % b.as_primitive()?),
            Arithmetic::Exp => BorrowObject::Primitive(a.as_primitive()?.exp(b.as_primitive()?)),
        };
        Ok(rst)
    } else {
        Err(ExprError::OtherErr(
            "invalid expression, the arithmetic operator misses operand".into(),
        ))
    }
}

fn apply_logical<'a>(
    logical: pb::Logical,
    first: Option<BorrowObject<'a>>,
    second: Option<BorrowObject<'a>>,
) -> ExprResult<BorrowObject<'a>> {
    if logical == Logical::Not {
        if let Some(a) = first {
            return Ok((!a.as_bool()?).into());
        }
    } else {
        if first.is_some() && second.is_some() {
            let a = first.unwrap();
            let b = second.unwrap();
            let rst = match logical {
                Logical::Eq => (a == b).into(),
                Logical::Ne => (a != b).into(),
                Logical::Lt => (a < b).into(),
                Logical::Le => (a <= b).into(),
                Logical::Gt => (a > b).into(),
                Logical::Ge => (a >= b).into(),
                Logical::And => (a.as_bool()? && b.as_bool()?).into(),
                Logical::Or => (a.as_bool()? || b.as_bool()?).into(),
                Logical::Not => unreachable!(),
                // todo within, without
                _ => unimplemented!(),
            };
            return Ok(rst);
        }
    }

    Err(ExprError::OtherErr(
        "invalid expression, the logical operator misses operand".into(),
    ))
}

// Private api
impl<'a> Evaluator<'a> {
    // Evaluate simple expression
    fn eval_without_stack(&'a self) -> ExprResult<Primitives> {
        assert!(self.suffix_tree.len() <= 3);
        if self.suffix_tree.is_empty() {
            return Err("empty expression".into());
        } else if self.suffix_tree.len() == 1 {
            return Ok(self.suffix_tree[0]
                .as_borrow_object()
                .ok_or(ExprError::from("invalid expression"))?
                .as_primitive()?);
        } else if self.suffix_tree.len() == 2 {
            // must be not
            if let Some(logical) = self.suffix_tree[1].as_logical() {
                return Ok(
                    apply_logical(logical, self.suffix_tree[0].as_borrow_object(), None)?
                        .as_primitive()?,
                );
            }
        } else {
            if let Some(logical) = self.suffix_tree[2].as_logical() {
                return Ok(apply_logical(
                    logical,
                    self.suffix_tree[0].as_borrow_object(),
                    self.suffix_tree[1].as_borrow_object(),
                )?
                .as_primitive()?);
            } else if let Some(arith) = self.suffix_tree[2].as_arith() {
                return Ok(apply_arith(
                    arith,
                    self.suffix_tree[0].as_borrow_object(),
                    self.suffix_tree[1].as_borrow_object(),
                )?
                .as_primitive()?);
            }
        }

        Err("invalid expression".into())
    }
}

impl<'a> Evaluator<'a> {
    /// Reset the status of the evaluator for further evaluation
    pub fn reset(&self) {
        self.stack.borrow_mut().clear();
    }

    /// Evaluate an expression without a context
    pub fn eval(&'a self) -> ExprResult<Primitives> {
        let mut stack = self.stack.borrow_mut();
        if self.suffix_tree.len() <= 3 {
            return self.eval_without_stack();
        }
        for opr in &self.suffix_tree {
            if opr.is_operand() {
                if let Some(obj) = opr.as_borrow_object() {
                    stack.push(obj);
                } else {
                    return Err("cannot obtain object from the operand".into());
                }
            } else {
                let first = stack.pop();
                if let Some(arith) = opr.as_arith() {
                    let rst = apply_arith(arith, stack.pop(), first)?;
                    stack.push(rst);
                } else if let Some(logical) = opr.as_logical() {
                    let rst = if logical == Logical::Not {
                        apply_logical(logical, first, None)?
                    } else {
                        apply_logical(logical, stack.pop(), first)?
                    };
                    stack.push(rst);
                } else {
                    return Err("invalid expression".into());
                }
            }
        }

        if stack.len() == 1 {
            Ok(stack.pop().unwrap().as_primitive()?)
        } else {
            Err("invalid expression".into())
        }
    }

    /// Evaluate an expression with a context to infer the value of variables
    pub fn eval_with_context(&'a self, _suffix_expr: &Vec<pb::ExprUnit>) -> ExprResult<Object> {
        todo!()
    }

    pub fn eval_bool(&'a self) -> ExprResult<bool> {
        let rst = self.eval()?.as_bool()?;
        Ok(rst)
    }

    pub fn eval_integer(&'a self) -> ExprResult<i32> {
        let rst = self.eval()?.as_i32()?;
        Ok(rst)
    }

    pub fn eval_long(&'a self) -> ExprResult<i64> {
        let rst = self.eval()?.as_i64()?;
        Ok(rst)
    }

    pub fn eval_float(&'a self) -> ExprResult<f64> {
        let rst = self.eval()?.as_f64()?;
        Ok(rst)
    }
}

impl pb::Const {
    pub fn as_object(&self) -> Option<Object> {
        use pb::value::Item::*;
        if let Some(val) = &self.value {
            val.item.as_ref().and_then(|item| match item {
                Boolean(b) => Some((*b).into()),
                I32(i) => Some((*i).into()),
                I64(i) => Some((*i).into()),
                F64(f) => Some((*f).into()),
                Str(s) => Some(s.clone().into()),
                Blob(blob) => Some(blob.clone().into()),
                None(_) => Option::None,
                I32Array(_) | I64Array(_) | F64Array(_) | StrArray(_) => {
                    unimplemented!()
                }
            })
        } else {
            Option::None
        }
    }

    pub fn as_borrow_object(&self) -> Option<BorrowObject> {
        use pb::value::Item::*;
        if let Some(val) = &self.value {
            val.item.as_ref().and_then(|item| match item {
                Boolean(b) => Some((*b).into()),
                I32(i) => Some((*i).into()),
                I64(i) => Some((*i).into()),
                F64(f) => Some((*f).into()),
                Str(s) => Some(s.as_str().into()),
                Blob(blob) => Some(blob.as_slice().into()),
                I32Array(_) | I64Array(_) | F64Array(_) | StrArray(_) => {
                    unimplemented!()
                }
                None(_) => Option::None,
            })
        } else {
            Option::None
        }
    }
}

impl pb::ExprUnit {
    pub fn as_object(&self) -> Option<Object> {
        self.item.as_ref().and_then(|item| match item {
            Item::Const(c) => c.as_object(),
            _ => None,
        })
    }

    pub fn as_borrow_object(&self) -> Option<BorrowObject> {
        self.item.as_ref().and_then(|item| match item {
            Item::Const(c) => c.as_borrow_object(),
            _ => None,
        })
    }

    pub fn is_operand(&self) -> bool {
        if let Some(item) = self.item.as_ref() {
            match item {
                Item::Const(_) | Item::Var(_) => true,
                _ => false,
            }
        } else {
            true
        }
    }

    pub fn as_arith(&self) -> Option<pb::Arithmetic> {
        self.item.as_ref().and_then(|item| match item {
            Item::Arith(arith) => Some(unsafe { std::mem::transmute::<_, pb::Arithmetic>(*arith) }),
            _ => None,
        })
    }

    pub fn as_logical(&self) -> Option<pb::Logical> {
        self.item.as_ref().and_then(|item| match item {
            Item::Logical(logi) => Some(unsafe { std::mem::transmute::<_, pb::Logical>(*logi) }),
            _ => None,
        })
    }
}
