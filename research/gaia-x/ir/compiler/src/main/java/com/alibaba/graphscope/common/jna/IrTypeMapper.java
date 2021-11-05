package com.alibaba.graphscope.common.jna;

import com.sun.jna.DefaultTypeMapper;

public class IrTypeMapper extends DefaultTypeMapper {
    public static IrTypeMapper INSTANCE = new IrTypeMapper();

    private IrTypeMapper() {
        super();
        addTypeConverter(IntEnum.class, new EnumConverter());
    }
}
