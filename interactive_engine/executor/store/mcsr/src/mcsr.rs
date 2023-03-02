use core::slice;
use std::alloc::Layout;
use std::fs::File;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::slice::Iter;
use std::{alloc, ptr};

use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::col_table::ColTable;
use crate::columns::{Column, Item};
use crate::graph::IndexType;

pub struct Nbr<I> {
    pub neighbor: I,
    pub offset: usize,
}

impl<I: IndexType> Clone for Nbr<I> {
    fn clone(&self) -> Self {
        Nbr { neighbor: I::new(self.neighbor.index()), offset: self.offset }
    }
}

pub struct NbrIter<'a, I> {
    begin: *const Nbr<I>,
    end: *const Nbr<I>,
    _marker: PhantomData<&'a Nbr<I>>,
}

impl<'a, I: IndexType> NbrIter<'a, I> {
    pub fn new_empty() -> Self {
        Self { begin: ptr::null(), end: ptr::null(), _marker: PhantomData }
    }

    #[inline]
    pub fn empty(&self) -> bool {
        self.begin == self.end
    }

    pub fn new_single(begin: *const Nbr<I>) -> Self {
        Self { begin, end: unsafe { begin.add(1) }, _marker: PhantomData }
    }

    pub fn slice(self, from: usize, to: usize) -> Self {
        let begin = unsafe { self.begin.offset(from as isize) };
        let end = unsafe { self.begin.offset(to as isize) };
        Self { begin, end, _marker: PhantomData }
    }
}

impl<'a, I: IndexType> Iterator for NbrIter<'a, I> {
    type Item = &'a Nbr<I>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.begin == self.end {
            None
        } else {
            unsafe {
                let cur = self.begin;
                self.begin = self.begin.offset(1);
                Some(&*cur)
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.begin = unsafe { self.begin.offset(n as isize) };
        self.next()
    }
}

unsafe impl<'a, I: IndexType> Send for NbrIter<'a, I> {}

unsafe impl<'a, I: IndexType> Sync for NbrIter<'a, I> {}

pub struct AdjList<I> {
    ptr: *mut Nbr<I>,
    pub(crate) deg: i64,
    pub(crate) cap: i64,
}

impl<I: IndexType> AdjList<I> {
    pub fn new() -> Self {
        AdjList { ptr: ptr::null_mut(), deg: 0, cap: 0 }
    }

    pub fn set(&mut self, ptr: *mut Nbr<I>, degree: i64, capacity: i64) {
        self.ptr = ptr;
        self.deg = degree;
        self.cap = capacity;
    }

    pub fn data(&self) -> *const Nbr<I> {
        self.ptr as *const Nbr<I>
    }

    pub fn degree(&self) -> i64 {
        self.deg
    }

    pub fn capacity(&self) -> i64 {
        self.cap
    }

    pub fn set_capacity(&mut self, capacity: i64) {
        self.cap = capacity;
    }

    pub fn extend(&mut self, delta_capacity: i64) {
        self.cap += delta_capacity;
    }

    pub fn put_edge(&mut self, id: I, offset: usize) {
        assert!(self.deg < self.cap);
        unsafe {
            ptr::write(
                self.ptr.add(self.deg as usize),
                Nbr::<I> { neighbor: id, offset: offset + self.deg as usize },
            );
        }
        self.deg += 1;
    }

    pub fn iter(&self) -> NbrIter<'_, I> {
        NbrIter {
            begin: self.ptr as *const Nbr<I>,
            end: unsafe { (self.ptr as *const Nbr<I>).add(self.deg as usize) },
            _marker: PhantomData,
        }
    }
}

impl<I> Clone for AdjList<I> {
    fn clone(&self) -> Self {
        AdjList { ptr: self.ptr, deg: self.deg, cap: self.cap }
    }
}

unsafe impl<I: IndexType> Send for AdjList<I> {}

unsafe impl<I: IndexType> Sync for AdjList<I> {}

pub struct MutableCsr<I> {
    buffers: Vec<(*mut u8, usize)>,
    adj_lists: Vec<AdjList<I>>,
    adj_offsets: Vec<usize>,
    edge_property: Option<ColTable>,
    prev: Vec<I>,
    next: Vec<I>,
    edge_num: usize,
}

pub struct MutableCsrEdgeIter<'a, I: IndexType> {
    cur_vertex: usize,
    adj_list_iter: Iter<'a, AdjList<I>>,
    nbr_iter: NbrIter<'a, I>,
}

impl<'a, I: IndexType> MutableCsrEdgeIter<'a, I> {
    pub fn new(
        adj_list: &'a Vec<AdjList<I>>, cur_vertex: usize, edge_property: &'a Option<ColTable>,
    ) -> Self {
        let mut adj_list_iter = adj_list.iter();
        if let Some(adj_list) = adj_list_iter.next() {
            Self { cur_vertex, adj_list_iter, nbr_iter: adj_list.iter() }
        } else {
            Self { cur_vertex, adj_list_iter, nbr_iter: NbrIter::<'a, I>::new_empty() }
        }
    }
}

impl<'a, I: IndexType> Iterator for MutableCsrEdgeIter<'a, I> {
    type Item = (I, &'a Nbr<I>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = self.nbr_iter.next() {
                return Some((I::new(self.cur_vertex), cur));
            } else {
                if let Some(adj_list) = self.adj_list_iter.next() {
                    self.cur_vertex += 1;
                    self.nbr_iter = adj_list.iter();
                } else {
                    self.nbr_iter = NbrIter::new_empty();
                    return None;
                }
            }
        }
    }
}

impl<I: IndexType> MutableCsr<I> {
    pub fn new() -> Self {
        MutableCsr {
            buffers: vec![],
            adj_lists: vec![],
            adj_offsets: vec![],
            edge_property: None,
            prev: vec![],
            next: vec![],
            edge_num: 0_usize,
        }
    }

    pub fn vertex_num(&self) -> I {
        I::new(self.adj_lists.len())
    }

    pub fn edge_num(&self) -> usize {
        self.edge_num
    }

    pub fn resize_vertices(&mut self, vnum: I) {
        if vnum == self.vertex_num() {
            return;
        }
        self.adj_lists
            .resize(vnum.index(), AdjList::<I>::new());
        self.adj_offsets.resize(vnum.index(), 0);
        self.prev
            .resize(vnum.index(), <I as IndexType>::max());
        self.next
            .resize(vnum.index(), <I as IndexType>::max());
    }

    pub fn reserve_edges_dense(&mut self, degree_to_add: &Vec<i64>) {
        let vnum = self.vertex_num().index();
        assert_eq!(degree_to_add.len(), vnum);
        self.adj_offsets.resize(vnum, 0);
        let mut new_buf_size: usize = 0;
        for i in 0..vnum {
            if i > 0 {
                self.adj_offsets[i] = self.adj_offsets[i - 1] + degree_to_add[i - 1] as usize;
            }
            if degree_to_add[i] == 0 {
                continue;
            }
            let requirement = self.adj_lists[i].degree() + degree_to_add[i];
            if requirement > self.adj_lists[i].capacity() {
                self.remove_node(I::new(i));
                // requirement += (requirement + 1) / 2;
                new_buf_size += requirement as usize;
                self.adj_lists[i].set_capacity(-requirement);
            }
        }
        if new_buf_size != 0 {
            let layout = Layout::array::<Nbr<I>>(new_buf_size).unwrap();
            let new_buf = unsafe { alloc::alloc(layout) };
            let mut p = <I as IndexType>::max();
            let mut begin = new_buf as *mut Nbr<I>;
            for i in 0..vnum {
                let mut cap = self.adj_lists[i].capacity();
                if cap < 0 {
                    cap = -cap;
                    self.adj_lists[i].set_capacity(cap);
                    self.prev[i] = p;
                    if p != <I as IndexType>::max() {
                        self.next[p.index()] = I::new(i);
                    }
                    p = I::new(i);
                    let old_degree = self.adj_lists[i].degree();
                    if old_degree > 0 {
                        unsafe { ptr::copy(self.adj_lists[i].data(), begin, old_degree as usize) };
                    }
                    self.adj_lists[i].set(begin, old_degree, cap);
                    begin = unsafe { begin.add(cap as usize) };
                }
            }
            if p != <I as IndexType>::max() {
                self.next[p.index()] = <I as IndexType>::max();
            }
            self.buffers.push((new_buf, new_buf_size));
        }
    }

    pub fn put_edge(&mut self, src: I, dst: I) {
        if src.index() < self.adj_lists.len() {
            self.edge_num += 1;
            self.adj_lists[src.index()].put_edge(dst, self.adj_offsets[src.index()]);
        }
    }

    pub fn put_edge_properties(&mut self, src: I, dst: I, properties: &Vec<Item>) {
        if src.index() < self.adj_lists.len() {
            let property_index = self.adj_offsets[src.index()] + self.adj_lists[src.index()].deg as usize;
            match self.edge_property.as_mut() {
                Some(edge_property) => edge_property.insert(property_index, properties),
                None => {}
            }
            self.edge_num += 1;
            self.adj_lists[src.index()].put_edge(dst, self.adj_offsets[src.index()]);
        }
    }

    pub fn update_degree(&mut self) {
        let vnum = self.adj_lists.len();
        self.adj_offsets.resize(vnum, 0);
        for i in 1..vnum {
            self.adj_offsets[i] = self.adj_offsets[i - 1] + self.adj_lists[i - 1].degree() as usize;
        }
    }

    pub fn put_col_table(&mut self, col_table: ColTable) {
        self.edge_property = Some(col_table);
    }

    fn remove_node(&mut self, v: I) {
        let p = self.prev[v.index()];
        let n = self.next[v.index()];
        if n == <I as IndexType>::max() && p == <I as IndexType>::max() {
            return;
        }
        let cap = self.adj_lists[v.index()].capacity();
        if p != <I as IndexType>::max() {
            self.adj_lists[p.index()].extend(cap);
            self.next[p.index()] = n;
        }
        if n != <I as IndexType>::max() {
            self.prev[n.index()] = p;
        }
    }

    pub fn degree(&self, src: I) -> i64 {
        if src.index() < self.adj_lists.len() {
            self.adj_lists[src.index()].degree()
        } else {
            0_i64
        }
    }

    pub fn get_edges(&self, src: I) -> Option<NbrIter<'_, I>> {
        if src.index() < self.adj_lists.len() {
            Some(self.adj_lists[src.index()].iter())
        } else {
            None
        }
    }

    pub fn get_all_edges(&self) -> MutableCsrEdgeIter<'_, I> {
        MutableCsrEdgeIter::new(&self.adj_lists, 0_usize, &self.edge_property)
    }

    pub fn get_properties(&self) -> Option<&ColTable> {
        self.edge_property.as_ref()
    }

    pub fn serialize(&self, path: &String) {
        assert_eq!(self.buffers.len(), 1_usize);

        let mut f = File::create(path).unwrap();
        let vnum = self.adj_lists.len();
        f.write_u64(vnum as u64).unwrap();

        let mut degree_vec = vec![0_i64; vnum];
        let mut capacity_vec = vec![0_i64; vnum];

        let mut total_capacity = 0_usize;
        for i in 0..vnum {
            degree_vec[i] = self.adj_lists[i].degree();
            capacity_vec[i] = self.adj_lists[i].capacity();
            total_capacity += capacity_vec[i] as usize;
        }

        f.write_u64(total_capacity as u64).unwrap();
        f.write_u64(self.edge_num as u64).unwrap();

        unsafe {
            let degree_vec_slice =
                slice::from_raw_parts(degree_vec.as_ptr() as *const u8, vnum * std::mem::size_of::<i64>());
            f.write_all(degree_vec_slice).unwrap();

            let capacity_vec_slice = slice::from_raw_parts(
                capacity_vec.as_ptr() as *const u8,
                vnum * std::mem::size_of::<i64>(),
            );
            f.write_all(capacity_vec_slice).unwrap();

            assert_eq!(total_capacity, self.buffers[0].1);
            let buffer_slice =
                slice::from_raw_parts(self.buffers[0].0, self.buffers[0].1 * std::mem::size_of::<Nbr<I>>());
            f.write_all(buffer_slice).unwrap();
        }

        if self.edge_property.is_some() {
            f.write_u64(1).unwrap();
            let property_path = path.clone() + "_PROPERTIES";
            self.edge_property
                .as_ref()
                .unwrap()
                .serialize_table(&property_path);
        } else {
            f.write_u64(0).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, path: &String) {
        println!("Start to open file {}", path);
        let mut f = File::open(path).unwrap();
        let vnum = f.read_u64().unwrap() as usize;
        let total_capacity = f.read_u64().unwrap() as usize;
        self.edge_num = f.read_u64().unwrap() as usize;

        let mut degree_vec = vec![0_i64; vnum];
        let mut capacity_vec = vec![0_i64; vnum];
        self.adj_offsets = vec![0; vnum];

        let buf_layout = Layout::array::<Nbr<I>>(total_capacity).unwrap();
        unsafe {
            let degree_vec_slice = slice::from_raw_parts_mut(
                degree_vec.as_mut_ptr() as *mut u8,
                vnum * std::mem::size_of::<i64>(),
            );
            f.read_exact(degree_vec_slice).unwrap();

            let capacity_vec_slice = slice::from_raw_parts_mut(
                capacity_vec.as_mut_ptr() as *mut u8,
                vnum * std::mem::size_of::<i64>(),
            );
            f.read_exact(capacity_vec_slice).unwrap();

            let buf = alloc::alloc(buf_layout);
            let buf_slice = slice::from_raw_parts_mut(buf, total_capacity * std::mem::size_of::<Nbr<I>>());
            f.read_exact(buf_slice).ok();

            self.buffers.push((buf, total_capacity));
        }

        let mut begin = self.buffers[0].0 as *mut Nbr<I>;
        self.adj_lists.resize(vnum, AdjList::<I>::new());
        self.prev.resize(vnum, <I as IndexType>::max());
        self.next.resize(vnum, <I as IndexType>::max());
        for i in 0..vnum {
            self.adj_lists[i].set(begin, degree_vec[i], capacity_vec[i]);
            begin = unsafe { begin.add(capacity_vec[i] as usize) };
        }

        self.update_degree();

        let have_properties = f.read_u64().unwrap();
        if have_properties == 1 {
            let property_path = path.clone() + "_PROPERTIES";
            let mut col_table = ColTable::new(vec![]);
            col_table.deserialize_table(&property_path);
            self.edge_property = Some(col_table);
        }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.adj_lists.len() != other.adj_lists.len() {
            return false;
        }
        let vnum = self.adj_lists.len();
        for i in 0..vnum {
            if self.adj_lists[i].degree() != other.adj_lists[i].degree() {
                return false;
            }
            let deg = self.adj_lists[i].degree();
            let mut iter1 = self.adj_lists[i].iter();
            let mut iter2 = other.adj_lists[i].iter();
            for _ in 0..deg {
                let v1 = iter1.next().unwrap();
                let v2 = iter2.next().unwrap();
                if v1.neighbor != v2.neighbor {
                    return false;
                }
            }
        }
        return true;
    }
}

impl<I> Drop for MutableCsr<I> {
    fn drop(&mut self) {
        for (ptr, buf_size) in self.buffers.iter() {
            let layout = Layout::array::<Nbr<I>>(*buf_size).unwrap();
            unsafe {
                alloc::dealloc(*ptr, layout);
            }
        }
        self.buffers.clear();
    }
}

unsafe impl<I: IndexType> Send for MutableCsr<I> {}

unsafe impl<I: IndexType> Sync for MutableCsr<I> {}

impl<I: IndexType> Encode for MutableCsr<I> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        let vnum = self.adj_lists.len();
        writer.write_u64(vnum as u64)?;
        for i in 0..vnum {
            writer.write_i64(self.adj_lists[i].deg)?;
        }
        let mut nbr_num = 0_usize;
        for i in 0..vnum {
            if let Some(edges) = self.get_edges(I::new(i)) {
                for e in edges {
                    e.neighbor.write_to(writer)?;
                    nbr_num += 1;
                }
            }
        }
        match self.edge_property.as_ref() {
            Some(edge_property) => {
                writer.write_i64(1_i64)?;
                edge_property.write_to(writer)?;
            }
            None => {
                writer.write_i64(0_i64)?;
            }
        }
        info!("write 1 u64, {} i64, {} nbr", vnum, nbr_num);
        Ok(())
    }
}

impl<I: IndexType> Decode for MutableCsr<I> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mut ret = Self::new();
        let vnum = reader.read_u64()? as usize;
        ret.resize_vertices(I::new(vnum));
        let mut degree_vec = Vec::with_capacity(vnum);
        for _ in 0..vnum {
            degree_vec.push(reader.read_i64()?);
        }
        ret.reserve_edges_dense(&degree_vec);
        for i in 0..vnum {
            for _ in 0..degree_vec[i] {
                let neighbor = I::read_from(reader)?;
                ret.put_edge(I::new(i), neighbor);
            }
        }

        if reader.read_i64().unwrap() == 1 {
            ret.put_col_table(ColTable::read_from(reader).unwrap());
        }

        Ok(ret)
    }
}

impl<I: IndexType> Serialize for MutableCsr<I> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut bytes = Vec::new();
        if self.write_to(&mut bytes).is_ok() {
            info!("writing {} bytes...", bytes.len());
            bytes.serialize(serializer)
        } else {
            Result::Err(S::Error::custom("Serialize mutable csr failed!"))
        }
    }
}

impl<'de, I> Deserialize<'de> for MutableCsr<I>
where
    I: IndexType,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<u8>::deserialize(deserializer)?;
        let mut bytes = vec.as_slice();
        MutableCsr::<I>::read_from(&mut bytes)
            .map_err(|_| D::Error::custom("Deserialize mutable csr failed!"))
    }
}
