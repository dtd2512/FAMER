package org.gradoop.famer.common.maxDeltaLinkSelection.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f2;f2->f1")

public class swapF1F2 implements MapFunction <Tuple3<Edge, String, String>, Tuple3<Edge, String, String>>{
    @Override
    public Tuple3<Edge, String, String> map(Tuple3<Edge, String, String> value) throws Exception {
        return Tuple3.of(value.f0, value.f2, value.f1);
    }
}
