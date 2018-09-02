package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")

public class getF0Tuple3<T0, T1, T2> implements MapFunction<Tuple3<T0, T1, T2>, T0> {
    @Override
    public T0 map(Tuple3<T0, T1, T2> in) throws Exception {
        return in.f0;
    }
}
