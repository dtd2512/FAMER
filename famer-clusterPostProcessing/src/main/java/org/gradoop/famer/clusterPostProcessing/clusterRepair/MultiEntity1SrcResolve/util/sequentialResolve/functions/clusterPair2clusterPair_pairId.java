package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class clusterPair2clusterPair_pairId implements FlatMapFunction<Tuple3<Cluster, Cluster, Boolean>, Tuple2<Tuple2<Cluster, Cluster>, String>> {
    private Boolean flag;
    public clusterPair2clusterPair_pairId (Boolean Flag) { flag = Flag;}
    @Override
    public void flatMap(Tuple3<Cluster, Cluster, Boolean> in, Collector<Tuple2<Tuple2<Cluster, Cluster>, String>> out) throws Exception {
        if (in.f2.equals(flag))
            out.collect(Tuple2.of(Tuple2.of(in.f0, in.f1), in.f0.getClusterId()+","+in.f1.getClusterId()));
    }
}
