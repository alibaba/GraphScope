package com.alibaba.maxgraph.v2.common.schema;

import com.alibaba.maxgraph.proto.v2.DataTypePb;
import org.apache.commons.lang3.StringUtils;

public enum DataType {
    /**
     * Unknown datatype
     */
    UNKNOWN(0),

    /**
     * boolean
     */
    BOOL(1),

    /**
     * character
     */
    CHAR(2),

    /**
     * short
     */
    SHORT(3),

    /**
     * integer
     */
    INT(4),

    /**
     * long
     */
    LONG(5),

    /**
     * float
     */
    FLOAT(6),

    /**
     * double
     */
    DOUBLE(7),

    /**
     * string
     */
    STRING(8),

    /**
     * byte array
     */
    BYTES(9),

    /**
     * integer list
     */
    INT_LIST(10),

    /**
     * long list
     */
    LONG_LIST(11),

    /**
     * float list
     */
    FLOAT_LIST(12),

    /**
     * double list
     */
    DOUBLE_LIST(13),

    /**
     * string list
     */
    STRING_LIST(14);

    private final byte type;

    DataType(int type) {
        this.type = (byte) type;
    }

    private static final DataType[] TYPES = DataType.values();

    public static DataType fromId(byte id) {
        if (id < 0 || id >= TYPES.length) {
            throw new IllegalArgumentException("Unknown DataType: [" + id + "]");
        }
        return TYPES[id];
    }

    public static DataType parseProto(DataTypePb pb) {
        return fromId((byte) pb.getNumber());
    }

    public static DataType parseString(String type) {
        String upperType = StringUtils.upperCase(type);
        if (StringUtils.startsWith(upperType, "LIST<")) {
            String typeValue = StringUtils.removeEnd(StringUtils.removeStart(upperType, "LIST<"), ">") + "_LIST";
            return DataType.valueOf(typeValue);
        } else {
            return DataType.valueOf(type);
        }
    }

    public DataTypePb toProto() {
        return DataTypePb.forNumber(type);
    }

    @Override
    public String toString() {
        String dataTypeString = this.name();
        if (StringUtils.endsWith(dataTypeString, "_LIST")) {
            String[] dataTypeArray = StringUtils.splitByWholeSeparator(dataTypeString, "_");
            dataTypeString = "LIST<" + dataTypeArray[0] + ">";
        }

        return dataTypeString;
    }

    public boolean isFixedLength() {
        switch (this) {
            case BOOL:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public int getTypeLength() {
        switch (this) {
            case BOOL:
                return 1;
            case CHAR:
                return 2;
            case SHORT:
                return 2;
            case INT:
                return 4;
            case LONG:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            default:
                throw new UnsupportedOperationException("not a fixed length type [" + this + "]");
        }
    }
}
