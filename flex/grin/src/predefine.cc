#include "grin/src/predefine.h"

GRIN_DATATYPE _get_data_type(const gs::PropertyType& type){
    if(type == gs::PropertyType::kInt32){
        return GRIN_DATATYPE::Int32;
    }else if(type == gs::PropertyType::kInt64){
        return GRIN_DATATYPE::Int64;
    }else if(type == gs::PropertyType::kString){
        return GRIN_DATATYPE::String;
    }else if(type == gs::PropertyType::kDate){
        return GRIN_DATATYPE::Timestamp64;
    }else if(type == gs::PropertyType::kDouble){
        return GRIN_DATATYPE::Double;
    }else {
        return GRIN_DATATYPE::Undefined;
    }

}