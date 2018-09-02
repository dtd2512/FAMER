package org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Transformation GroupReduceFunction function to replicate WindowSize-1 vertices for the next window

 * input: a groups of vertices
 * output: several vertices
 */
public class Replicate implements GroupReduceFunction<Tuple3<Vertex, String, Integer>, Tuple3<Vertex, String, Integer>> {
    private Integer WindowSize;

    public Replicate(Integer windowSize) {
        WindowSize = windowSize;
    }
    public void reduce(Iterable<Tuple3<Vertex, String, Integer>> in, Collector<Tuple3<Vertex, String, Integer>> out) {
        int cnt = 0;
        for (Tuple3<Vertex, String, Integer> i : in) {
            cnt++;
            if (cnt <= WindowSize-1)
                out.collect(i);
        }
    }
}