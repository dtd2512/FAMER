package org.gradoop.famer.initialGraphGeneration.vertexGeneration;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.beans.VetoableChangeListener;

/**
 */
public class assignGraphId implements MapFunction <Vertex, Vertex>{
    private GradoopId graphId;
    public assignGraphId (GradoopId GraphId) { graphId = GraphId;}
    @Override
    public Vertex map(Vertex vertex) throws Exception {
        GradoopIdList gradoopIds = new GradoopIdList();
        gradoopIds.add(graphId);
        vertex.setGraphIds(gradoopIds);
        return vertex;
    }
}
