package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")

public class getTuple1F1Tuple2<T0, T1> implements MapFunction<Tuple2<T0,T1>, Tuple1<T1>> {
    @Override
    public Tuple1<T1> map(Tuple2<T0, T1> in) throws Exception {
        return Tuple1.of(in.f1);
    }
}
