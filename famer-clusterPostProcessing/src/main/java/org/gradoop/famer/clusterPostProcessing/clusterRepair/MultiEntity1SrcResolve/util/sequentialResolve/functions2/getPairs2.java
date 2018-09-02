package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */

public class getPairs2 implements MapFunction<Tuple4<Cluster, Cluster, String, Double>, Tuple2<Cluster, Cluster>> {
    @Override
    public Tuple2<Cluster, Cluster> map(Tuple4<Cluster, Cluster, String, Double> value) throws Exception {
        return Tuple2.of(value.f0, value.f1);
    }
}
