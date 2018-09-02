package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;



public class ComputePartitionEnumerationStartPoint implements GroupReduceFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {
    @Override
    public void reduce(Iterable<Tuple3<String, Integer, Long>> in, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
        Long totalSum = 0l;
        for (Tuple3<String, Integer, Long> i:in){
            out.collect(Tuple3.of(i.f0, i.f1, totalSum));
            totalSum += i.f2;
        }
    }
}

