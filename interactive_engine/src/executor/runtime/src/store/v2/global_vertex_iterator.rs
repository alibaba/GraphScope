use store::LocalStoreVertex;
use std::iter::Map;
use std::vec::IntoIter;

pub struct GlobalVertexIteratorImpl {
    vec_iter: IntoIter<LocalStoreVertex>,
}

impl Iterator for GlobalVertexIteratorImpl {
    type Item = LocalStoreVertex;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

impl GlobalVertexIteratorImpl {
    pub fn new(iter: Vec<LocalStoreVertex>) -> Self {
        GlobalVertexIteratorImpl {
            vec_iter: iter.into_iter(),
        }
    }
}

// impl<T: Iterator<Item=LocalStoreVertex>> GlobalVertexIteratorImpl {
//     pub fn new(iter: T) -> Self {
//         GlobalVertexIteratorImpl {
//
//         }
//     }
// }

// pub struct GlobalVertexIteratorImpl<V, VI, E, EI>
//     where V: Vertex,
//           VI: Iterator<Item=V>,
//           E: Edge,
//           EI: Iterator<Item=E> {
//     wrapped_iter: Iterator<Item=E>,
//     parse_fn: fn(E) -> V,
// }
//
// impl<V, VI, E, EI> Iterator for GlobalVertexIteratorImpl<V, VI, E, EI>
//     where V: Vertex,
//           VI: Iterator<Item=V>,
//           E: Edge,
//           EI: Iterator<Item=E> {
//     type Item = GlobalVertexImpl;
//     type V = GlobalVertexImpl;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         match self.wrapped_iter.next() {
//             None => {
//                 None
//             },
//             Some(ori) => {
//                 self.parse_fn(ori)
//             },
//         }
//     }
// }
//
// impl<E, R> GlobalVertexIteratorImpl<E, R> {
//     pub fn new(iters: Vec<Box<dyn EdgeResultIter>>, parse_fn: fn(T)->R) -> Self {
//         let wrapped_iter = IteratorList::new(iters);
//         GlobalVertexIteratorImpl {
//             wrapped_iter,
//             parse_fn,
//         }
//     }
// }
