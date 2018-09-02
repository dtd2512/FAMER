package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by alieh on 4/21/17.
 */
public class AssignVertexIndex implements GroupReduceFunction<Tuple4<Vertex, String, Integer, Long>, Tuple3<Vertex, String, Long>> {
@Override
        public void reduce(Iterable<Tuple4<Vertex, String, Integer, Long>> in, Collector<Tuple3<Vertex, String, Long>> out) throws Exception {
            Long cnt = 0l;
            for (Tuple4<Vertex, String, Integer, Long> i:in){
                out.collect(Tuple3.of(i.f0, i.f1, cnt+i.f3));
                cnt++;
            }
        }
}