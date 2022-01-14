use std::ops::Deref;

use nohash_hasher::IntMap;
use rand::Rng;

pub struct IdTopo<B: Deref<Target = [u64]>> {
    vertices: IntMap<u64, (u64, u64)>,
    neighbors: B,
}

impl<B: Deref<Target = [u64]>> IdTopo<B> {
    pub fn new(vertices: IntMap<u64, (u64, u64)>, neighbors: B) -> Self {
        IdTopo { vertices, neighbors }
    }

    pub fn total_vertices(&self) -> usize {
        self.vertices.len()
    }

    pub fn total_edges(&self) -> usize {
        self.neighbors.len()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &u64> {
        self.vertices.keys()
    }

    #[inline]
    pub fn get_neighbors(&self, id: u64) -> Neighbors {
        if let Some((offset, len)) = self.vertices.get(&id) {
            if *len == 0 {
                Neighbors::empty()
            } else {
                let start = *offset as usize;
                let ptr = unsafe { self.neighbors.as_ptr().add(start) };
                Neighbors::new(ptr, *len as usize)
            }
        } else {
            Neighbors::empty()
        }
    }

    // depth firsh search k-hop neighbors;
    pub fn get_k_hop_neighbors(&self, id: u64, k: u8) -> Box<dyn Iterator<Item = u64>> {
        if k == 0 {
            return Box::new(std::iter::empty());
        }

        let n = self.get_neighbors(id);
        if k == 1 {
            return Box::new(n);
        }
        let kn = k - 1;
        let mut res = vec![];
        for v in n {
            res.push(self.get_k_hop_neighbors(v, kn))
        }
        Box::new(res.into_iter().flat_map(|n| n))
    }

    #[inline]
    pub fn count_neighbors(&self, id: u64) -> usize {
        if let Some((_, len)) = self.vertices.get(&id) {
            *len as usize
        } else {
            0
        }
    }

    pub fn sample_vertices(&self, size: usize, ratio: f64) -> Vec<u64> {
        if size > 0 {
            let mut rng = rand::thread_rng();
            let mut sample = Vec::with_capacity(size);
            for x in self.vertices.keys() {
                if rng.gen_bool(ratio) {
                    sample.push(*x);
                    if sample.len() == size {
                        return sample;
                    }
                }
            }
            sample
        } else {
            vec![]
        }
    }
}

lazy_static! {
    static ref EMPTY_VEC: Vec<u64> = vec![];
}

pub struct Neighbors {
    ptr: *const u64,
    len: usize,
    next: usize,
}

unsafe impl Send for Neighbors {}

impl Neighbors {
    fn new(ptr: *const u64, len: usize) -> Self {
        Neighbors { ptr, len, next: 0 }
    }

    pub fn empty() -> Self {
        let ptr = EMPTY_VEC.as_ptr();
        Neighbors::new(ptr, 0)
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Iterator for Neighbors {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.len {
            None
        } else {
            if self.ptr.is_null() {
                panic!("null point error;")
            }
            let mut ptr = self.ptr;
            let next = self.next;
            self.next += 1;
            Some(unsafe {
                ptr = ptr.add(next);
                if ptr.is_null() {
                    panic!("null point error;")
                }
                *ptr as u64
            })
        }
    }
}
