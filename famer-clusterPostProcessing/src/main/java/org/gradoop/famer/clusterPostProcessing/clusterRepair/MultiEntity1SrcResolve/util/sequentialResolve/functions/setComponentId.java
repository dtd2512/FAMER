package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class setComponentId implements MapFunction <Vertex, Vertex>{
    @Override
    public Vertex map(Vertex value) throws Exception {
        value.setProperty("ComponentId", value.getPropertyValue("ClusterId").toString());
        return value;
    }
}
