package org.gradoop.famer.clusterPostProcessing.clusterRepair.functions;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class removeLinks2PerfectClusters implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Edge> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Edge> out) throws Exception {
        String srcClusterId = in.f1.getPropertyValue("ClusterId").toString();
        String trgtClusterId = in.f2.getPropertyValue("ClusterId").toString();
        if (srcClusterId.equals(trgtClusterId))
            out.collect(in.f0);
        else {
            Boolean srcStatus = Boolean.parseBoolean(in.f1.getPropertyValue("isCompletePerfect").toString());
            Boolean trgtStatus = Boolean.parseBoolean(in.f2.getPropertyValue("isCompletePerfect").toString());
            if ((!srcStatus && !trgtStatus)) {
                out.collect(in.f0);
            }
        }

    }
}
