package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class ComputeBlockSize implements GroupReduceFunction<Tuple3<String, Integer, Long>, Tuple2<String, Long>> {
    @Override
    public void reduce(Iterable<Tuple3<String, Integer, Long>> in, Collector<Tuple2<String, Long>> out) throws Exception {
        String key = "";
        Long cnt = 0l;
        for (Tuple3<String, Integer, Long> i:in){
            key = i.f0;
            cnt += i.f2;
        }
        out.collect(Tuple2.of(key, cnt));
    }
}