use crate::db::api::*;
use super::super::table_manager::{Table};
use super::super::types::*;

pub trait TableInfoTest {
    fn get_table(&self, si: SnapshotId) -> Option<Table>;

    /// make sure that tables is in increasing order by start_si
    fn test(&self, tables: Vec<Table>) {
        if tables.len() == 0 {
            return;
        }
        let min_si = if tables[0].start_si > 10 { tables[0].start_si - 10 } else { 1 };
        let max_si = tables.last().unwrap().start_si;
        let mut si_list = vec![-1; (max_si - min_si + 1) as usize];
        for i in 0..tables.len() {
            let t = &tables[i];
            si_list[(t.start_si - min_si) as usize] = i as SnapshotId;
        }
        for i in 1..si_list.len() {
            if si_list[i] == -1 {
                si_list[i] = si_list[i-1];
            }
        }

        for i in 0..si_list.len() {
            let res = self.get_table(min_si + i as SnapshotId);
            if si_list[i] == -1 {
                if res.is_some() {
                    println!("i: {}, min_si: {}, table: {}", i, min_si, tables[si_list[i] as usize].id);
                }
                assert!(res.is_none());
            } else {
                assert_eq!(res.unwrap(), tables[si_list[i] as usize]);
            }
        }
    }
}

pub struct VertexInfoTest {
    info: VertexTypeInfoRef,
}

impl VertexInfoTest {
    pub fn new(info: VertexTypeInfoRef) -> Self {
        VertexInfoTest {
            info,
        }
    }
}

impl TableInfoTest for VertexInfoTest {
    fn get_table(&self, si: i64) -> Option<Table> {
        self.info.get_table(si)
    }
}

pub struct EdgeTypeInfoTest {
    info: EdgeKindInfoRef,
}

impl EdgeTypeInfoTest {
    pub fn new(info: EdgeKindInfoRef) -> Self {
        EdgeTypeInfoTest {
            info,
        }
    }
}

impl TableInfoTest for EdgeTypeInfoTest {
    fn get_table(&self, si: i64) -> Option<Table> {
        self.info.get_table(si)
    }
}

pub fn create_test_type_def(label: LabelId) -> TypeDef {
    let types = ValueType::all_value_types();
    let mut builder = TypeDefBuilder::new();
    for x in label..label + 10 {
        let id = (x * 10 + 1) as PropertyId;
        let inner_id = id * 2 - 1;
        let r#type = types[x as usize % types.len()];
        builder.add_property(id, inner_id, id.to_string(), r#type, None, false, "cmt".to_string());
    }
    builder.set_label_id(label);
    builder.build()
}

pub fn create_full_type_def(label: LabelId) -> TypeDef {
    let types = ValueType::all_value_types();
    let mut builder = TypeDefBuilder::new();
    for i in 0..types.len() {
        let id = i as PropertyId + 1;
        let inner_id = id * 2 - 1;
        let r#type = types[i];
        builder.add_property(id, inner_id, id.to_string(), r#type, None, false, "cmt".to_string());
    }
    builder.set_label_id(label);
    builder.build()
}

pub fn create_edge_kind(si: SnapshotId, label: LabelId) -> EdgeKind {
    let src_label = label * 100 + si as LabelId - 1;
    let dst_label = src_label + 1;
    EdgeKind::new(label, src_label, dst_label)
}
