package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class updateOverlappedClusters implements JoinFunction <Tuple2<Cluster, String>, Tuple2<Cluster, String>, Tuple2<Cluster, String>> {
    @Override
    public Tuple2<Cluster, String> join(Tuple2<Cluster, String> in1, Tuple2<Cluster, String> in2) throws Exception {
        if (in1==null)
            return in2;
        return in1;
    }
}
