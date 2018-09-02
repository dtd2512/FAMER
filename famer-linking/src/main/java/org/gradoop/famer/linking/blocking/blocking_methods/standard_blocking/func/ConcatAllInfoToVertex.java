package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by alieh on 4/21/17.
 */

public class ConcatAllInfoToVertex implements JoinFunction<Tuple3<Vertex, String, Long>, Tuple5<String, Long, Long, Long, Long>, Tuple6<Vertex, String, Long, Long, Long, Long>> {
    public Tuple6<Vertex, String, Long, Long, Long, Long> join(Tuple3<Vertex, String, Long> in1, Tuple5<String, Long, Long, Long, Long> in2) throws Exception {
        return Tuple6.of(in1.f0, in1.f1, in1.f2, in2.f1, in2.f3, in2.f4);
    }
}
