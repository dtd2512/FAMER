package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class computeOverlappedLength implements FlatMapFunction<Vertex, Tuple1<Integer>> {
    @Override
    public void flatMap(Vertex value, Collector<Tuple1<Integer>> out) throws Exception {
        Integer overlapLength = value.getPropertyValue("ClusterId").toString().split(",").length;
        if (overlapLength > 1)
            out.collect(Tuple1.of(overlapLength));
    }
}
