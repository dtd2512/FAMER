package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class clusterPair2clusterPair_clusterId implements FlatMapFunction <Tuple5<Cluster, Cluster, String, Integer, Double>, Tuple3<Cluster, Cluster, String>>{
    @Override
    public void flatMap(Tuple5<Cluster, Cluster, String, Integer, Double> in, Collector<Tuple3<Cluster, Cluster, String>> out) throws Exception {

        out.collect(Tuple3.of(in.f0, in.f1, in.f0.getClusterId()));
        out.collect(Tuple3.of(in.f0, in.f1, in.f1.getClusterId()));
    }
}
