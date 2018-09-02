package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */

public class updateOverlappedVertices implements JoinFunction<Tuple2<Vertex, String>, Tuple2<Vertex, String>, Tuple2<Vertex, String>> {
    @Override
    public Tuple2<Vertex, String> join(Tuple2<Vertex, String> in1, Tuple2<Vertex, String> in2) throws Exception {
        if (in2==null)
            return in1;
        return in2;
    }
}
