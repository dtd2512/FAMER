package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class ConcatVertextoPartitionInfo implements JoinFunction<Tuple3<String, Integer, Long>, Tuple3<Vertex, String, Integer>, Tuple4<Vertex, String, Integer, Long>> {
    public Tuple4<Vertex, String, Integer, Long> join(Tuple3<String, Integer, Long> in1, Tuple3<Vertex, String, Integer> in2) throws Exception {
        return Tuple4.of(in2.f0, in2.f1, in2.f2, in1.f2);
    }
}
