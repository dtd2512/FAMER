package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class cluster2interLink extends RichFlatMapFunction<Cluster, Edge> {
    @Override
    public void flatMap(Cluster input, Collector<Edge> out) throws Exception {
        for (Edge e: input.getInterLinks())
            out.collect(e);
    }
}
