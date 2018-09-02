package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by alieh on 4/21/17.
 */
public class EnumeratePartitionEntities implements GroupCombineFunction<Tuple3<Vertex, String, Integer>, Tuple3<String, Integer, Long>>{
    @Override
    public void combine(Iterable<Tuple3<Vertex, String, Integer>> in, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
        String key = "";
        Long cnt = 0l;
        Integer pid = 0;
        for (Tuple3<Vertex, String, Integer> i:in){
            key = i.f1;
            pid = i.f2;
            cnt++;
        }
        out.collect(Tuple3.of(key, pid, cnt));

    }
}
