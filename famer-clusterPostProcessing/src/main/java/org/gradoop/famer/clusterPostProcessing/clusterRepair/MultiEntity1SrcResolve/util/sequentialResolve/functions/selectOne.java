package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class selectOne implements GroupReduceFunction <Tuple3<Cluster, Cluster, String>, Tuple3<Cluster, Cluster, Boolean>>{
    @Override
    public void reduce(Iterable<Tuple3<Cluster, Cluster, String>> in, Collector<Tuple3<Cluster, Cluster, Boolean>> out) throws Exception {
        boolean isFirst = true;
        for (Tuple3<Cluster, Cluster, String> i: in){
            if (isFirst) {
                out.collect(Tuple3.of(i.f0, i.f1, true));
                isFirst = false;
            }
            else
                out.collect(Tuple3.of(i.f0, i.f1, false));
        }
    }
}
