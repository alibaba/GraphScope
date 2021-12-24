package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.AuxiliaOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Function;

public class AliasTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    @Test
    public void selectAliasTest() {
        InterOpCollection opCollection = new InterOpCollection();

        SelectOp op = new SelectOp();
        op.setPredicate(new OpArg("@.name == \"marko\"", Function.identity()));
        FfiAlias.ByValue tag = ArgUtils.asFfiAlias("a", true);
        op.setAlias(new OpArg(tag, Function.identity()));
        opCollection.appendInterOp(op);

        AliasProcessor.INSTANCE.process(opCollection);
        AuxiliaOp actual = (AuxiliaOp) opCollection.unmodifiableCollection().get(1);
        Assert.assertEquals(tag, actual.getAlias().get().getArg());
    }

    @Test
    public void propertyDetailsAliasTest() {
        InterOpCollection opCollection = new InterOpCollection();

        SelectOp selectOp = new SelectOp();
        selectOp.setPredicate(new OpArg("@.name == \"marko\"", Function.identity()));
        FfiAlias.ByValue tag = ArgUtils.asFfiAlias("a", true);
        selectOp.setAlias(new OpArg(tag, Function.identity()));
        opCollection.appendInterOp(selectOp);

        AuxiliaOp auxiliaOp = new AuxiliaOp();
        auxiliaOp.setPropertyDetails(new OpArg(Arrays.asList(irCoreLib.cstrAsNameOrId("name")), Function.identity()));
        opCollection.appendInterOp(auxiliaOp);

        AliasProcessor.INSTANCE.process(opCollection);

        Assert.assertEquals(2, opCollection.unmodifiableCollection().size());
        AuxiliaOp actual = (AuxiliaOp) opCollection.unmodifiableCollection().get(1);
        Assert.assertEquals(tag, actual.getAlias().get().getArg());
    }
}
