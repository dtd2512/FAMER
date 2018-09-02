package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */

public class tuple2Vertex2tuple3Vertex_ids implements MapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, String>> {
    @Override
    public Tuple3<Vertex, Vertex, String> map(Tuple2<Vertex, Vertex> input) throws Exception {
        String id1 = input.f0.getId().toString();
        String id2 = input.f1.getId().toString();
        if (id1.compareTo(id2) < 0)
            return Tuple3.of(input.f0, input.f1, id1+","+ id2);
        else
            return Tuple3.of(input.f1, input.f0, id2+","+ id1);
    }
}
