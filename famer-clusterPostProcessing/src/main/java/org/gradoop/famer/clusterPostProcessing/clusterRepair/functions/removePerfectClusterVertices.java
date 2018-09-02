package org.gradoop.famer.clusterPostProcessing.clusterRepair.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class removePerfectClusterVertices implements FlatMapFunction <Vertex, Vertex> {
    @Override
    public void flatMap(Vertex in, Collector<Vertex> out) throws Exception {
        if (!Boolean.parseBoolean(in.getPropertyValue("isCompletePerfect").toString()))
            out.collect(in);
    }
}
