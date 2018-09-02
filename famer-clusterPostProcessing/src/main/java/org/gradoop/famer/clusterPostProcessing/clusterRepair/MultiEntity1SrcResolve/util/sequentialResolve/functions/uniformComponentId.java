package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class uniformComponentId implements GroupReduceFunction <Tuple3<Cluster, String, String>, Cluster>{
    @Override
    public void reduce(Iterable<Tuple3<Cluster, String, String>> in, Collector<Cluster> out) throws Exception {
        Cluster c = null;
        String componentId = "";
        Boolean firstRecoed = true;
        for (Tuple3<Cluster, String, String> i:in){
            if (firstRecoed){
                firstRecoed = false;
                c = i.f0;
                componentId = i.f2;
            }
            else {
                if (i.f2.compareTo(componentId) < 0)
                    componentId = i.f2;
            }

        }
        c.setComponentId(componentId);
        out.collect(c);
    }
}
