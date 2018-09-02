package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class cluster2cluster_interLinkId implements FlatMapFunction <Cluster, Tuple2<Cluster, String>>{
    @Override
    public void flatMap(Cluster input, Collector<Tuple2<Cluster, String>> out) throws Exception {

        for (Edge e: input.getInterLinks()) {
            out.collect(Tuple2.of(input, e.getId().toString()));
        }

    }
}
