package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class setClusterId implements MapFunction <Vertex, Vertex>{
    @Override
    public Vertex map(Vertex in) throws Exception {
        in.setProperty("ClusterId", "r"+in.getPropertyValue("VertexPriority").toString());
        return in;
    }
}
