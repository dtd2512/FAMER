package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class overlapLength implements FlatMapFunction<Vertex, Tuple2<Vertex, Integer>> {

    @Override
    public void flatMap(Vertex input, Collector<Tuple2<Vertex, Integer>> out) throws Exception {
        if (input.getPropertyValue("ClusterId").toString().contains(","))
            out.collect(Tuple2.of(input, input.getPropertyValue("ClusterId").toString().split(",").length));
    }
}
