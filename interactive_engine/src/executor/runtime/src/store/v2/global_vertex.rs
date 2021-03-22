use crate::db::api::{VertexId, LabelId, ValueRef, ValueType, PropertiesRef};
use crate::db::graph::property::{PropData, PropertiesIter};
use crate::db::graph::codec::Decoder;
use crate::api::Vertex;
use crate::api::prelude::Property;

pub struct GlobalVertexImpl {
    id: VertexId,
    label_id: LabelId,
    data: PropData<'static>,
    decoder: Decoder,
}

impl Vertex for GlobalVertexImpl {
    type PI = PropertiesIter<'static>;

    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_label_id(&self) -> u32 {
        u32::from(self.label_id)
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        match self.decoder.decode_property(self.data.as_bytes(), i32::from(prop_id)) {
            None => { None },
            Some(val_ref) => {
                GlobalVertexImpl::parse_val_ref(val_ref)
            },
        }
    }

    fn get_properties(&self) -> Self::PI {
        let data = unsafe { std::mem::transmute(self.data.as_bytes()) };
        let iter = self.decoder.decode_properties(data);
        PropertiesIter::new(iter)
    }
}

impl GlobalVertexImpl {
    pub fn new() -> Self {

    }

    fn parse_val_ref(val_ref: ValueRef) -> Property {
        match val_ref.get_type() {
            ValueType::Bool => {
                Property::Bool(val_ref.get_bool()?)
            },
            ValueType::Char => {
                Property::Char(val_ref.get_char()?)
            },
            ValueType::Short => {
                Property::Short(val_ref.get_short()?)
            },
            ValueType::Int => {
                Property::Int(val_ref.get_int()?)
            },
            ValueType::Long => {
                Property::Long(val_ref.get_long()?)
            },
            ValueType::Float => {
                Property::Float(val_ref.get_float()?)
            },
            ValueType::Double => {
                Property::Double(val_ref.get_double()?)
            },
            ValueType::String => {
                Property::String(String::from(val_ref.get_str()?))
            },
            ValueType::Bytes => {
                Property::Bytes(Vec::from(val_ref.get_bytes()?))
            },
            ValueType::IntList => {
                Property::ListInt(val_ref.get_int_list()?.iter().collect())
            },
            ValueType::LongList => {
                Property::ListLong(val_ref.get_long_list()?.iter().collect())
            },
            ValueType::FloatList => {
                Property::ListFloat(val_ref.get_float_list()?.iter().collect())
            },
            ValueType::DoubleList => {
                Property::ListDouble(val_ref.get_double_list()?.iter().collect())
            },
            ValueType::StringList => {
                Property::ListString(val_ref.get_str_list()?.iter().collect())
            },
        }
    }
}
