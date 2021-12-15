package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import com.alibaba.graphscope.common.jna.type.FfiVariable;

public class ArgUtils {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static String LABEL = "~label";
    private static String ID = "~id";
    private static final String HIDDEN_PREFIX = "~";

    public static FfiNameOrId.ByValue strAsNameId(String value) {
        return irCoreLib.cstrAsNameOrId(value);
    }

    public static FfiConst.ByValue intAsConst(int id) {
        return irCoreLib.int32AsConst(id);
    }

    public static FfiConst.ByValue longAsConst(long id) {
        return irCoreLib.int64AsConst(id);
    }

    public static FfiProperty.ByValue asFfiProperty(String key) {
        if (key.equals(LABEL)) {
            return irCoreLib.asLabelKey();
        } else if (key.equals(ID)) {
            return irCoreLib.asIdKey();
        } else {
            return irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId(key));
        }
    }

    public static FfiVariable.ByValue asVarPropertyOnly(FfiProperty.ByValue property) {
        return irCoreLib.asVarPropertyOnly(property);
    }

    public static FfiVariable.ByValue asNoneVar() {
        return irCoreLib.asNoneVar();
    }

    public static String asHiddenStr(String value) {
        return HIDDEN_PREFIX + value;
    }

    public static boolean isHiddenStr(String value) {
        return value != null && value.startsWith(HIDDEN_PREFIX);
    }
}

