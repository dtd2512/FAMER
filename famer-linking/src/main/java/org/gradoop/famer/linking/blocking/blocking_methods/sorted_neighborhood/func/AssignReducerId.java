package org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Transformation FlatMapFunction function to assign an id called in this program reducerId to each vertex

 * input: a vertex
 * output: a vertex and a reducerId
 */
public class AssignReducerId implements FlatMapFunction<Tuple2<Vertex, String>, Tuple3<Vertex, String, Integer>> {
    private Integer interval;
    private Integer reducerNo;

    public AssignReducerId(Integer ReducerNo) {
        reducerNo = ReducerNo;
        reducerNo = Math.min(26,reducerNo);
        interval = 26/ReducerNo;

    }
    public void flatMap(Tuple2<Vertex, String> in, Collector<Tuple3<Vertex, String, Integer>> out) {
        char initial = in.f1.charAt(0);
        for (int i=1; i<=reducerNo;i++){
            if (initial<97+i*interval || i==reducerNo) {
                out.collect(Tuple3.of(in.f0, in.f1, i));
                break;
            }
        }
    }
}
