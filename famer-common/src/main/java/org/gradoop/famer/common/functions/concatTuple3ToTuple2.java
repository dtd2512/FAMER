package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")

public class concatTuple3ToTuple2<T> implements MapFunction <Tuple3<T, String, String>, Tuple2<T, String>>{
    @Override
    public Tuple2<T, String> map(Tuple3<T, String, String> value) throws Exception {
        return Tuple2.of(value.f0, value.f1 + "," + value.f2);
    }
}
