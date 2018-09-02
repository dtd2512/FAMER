package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class Cluster2Vertex implements FlatMapFunction<Cluster, Cluster>{
    @Override
    public void flatMap(Cluster cluster, Collector<Cluster> out) throws Exception {
        boolean isOut=false;
        for (Vertex v: cluster.getVertices()){
            if (v.getPropertyValue("ClusterId").toString().contains(",")){
                System.out.println("********************************************"+cluster.getClusterId());
                isOut=true;
                break;
            }
        }
        if (isOut)
            out.collect(cluster);
    }
}
