use dyn_type::Object;
use ir_core::expr::eval::{Context, Evaluator};
use ir_core::expr::to_suffix_expr_pb;
use ir_core::expr::token::tokenize;
use ir_core::graph::element::Vertex;
use ir_core::graph::property::{DefaultDetails, DynDetails, Label};
use ir_core::NameOrId;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::sync::Arc;

struct Vertices {
    vec: Vec<Vertex>,
}

impl Context<Vertex> for Vertices {
    fn get(&self, key: &NameOrId) -> Option<&Vertex> {
        match key {
            NameOrId::Str(_) => None,
            NameOrId::Id(i) => self.vec.get(*i as usize),
        }
    }
}

fn main() {
    // "((1 + 2) + 3) * (7 * 8) + 12.5 / 10.1"
    let expr_str = std::env::args().nth(1).unwrap();
    let times: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let tokens = tokenize(&expr_str).unwrap();
    let suffix_tree = to_suffix_expr_pb(tokens).unwrap();
    let eval = Evaluator::try_from(suffix_tree).unwrap();
    let mut count = 0;
    let start = std::time::Instant::now();
    let map1: HashMap<NameOrId, Object> = vec![
        (NameOrId::from("age".to_string()), 31.into()),
        (NameOrId::from("birthday".to_string()), 19900416.into()),
        (
            NameOrId::from("name".to_string()),
            "John".to_string().into(),
        ),
    ]
    .into_iter()
    .collect();
    let map2: HashMap<NameOrId, Object> = vec![
        (NameOrId::from("age".to_string()), 26.into()),
        (NameOrId::from("birthday".to_string()), 19950816.into()),
        (
            NameOrId::from("name".to_string()),
            "Nancy".to_string().into(),
        ),
    ]
    .into_iter()
    .collect();

    let ctxt = Vertices {
        vec: vec![
            Vertex::new(DynDetails::new(DefaultDetails::with_property(
                1,
                Label::from(1),
                map1,
            ))),
            Vertex::new(DynDetails::new(DefaultDetails::with_property(
                2,
                Label::from(2),
                map2,
            ))),
        ],
    };
    for _ in 0..times {
        count += eval.eval::<_, _>(Some(&ctxt)).unwrap().as_i32().unwrap();
    }
    println!(
        "count: {:?}, elapsed: {:?}",
        count,
        start.elapsed().as_millis()
    );
}
