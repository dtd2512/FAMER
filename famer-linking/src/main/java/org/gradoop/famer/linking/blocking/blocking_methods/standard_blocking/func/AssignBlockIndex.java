package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 */
@FunctionAnnotation.ForwardedFields("f0->f2")

public class AssignBlockIndex implements MapFunction <Tuple2<Long, Tuple2<String, Long>>, Tuple3<String, Long, Long>> {

    public Tuple3<String, Long, Long> map(Tuple2<Long, Tuple2<String, Long>> in) throws Exception {
        return Tuple3.of(in.f1.f0, in.f1.f1, in.f0);
    }
}
