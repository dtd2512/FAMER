package org.gradoop.famer.linking.linking.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class createLink implements MapFunction<Tuple3<Vertex, Vertex, Double>, Edge>{
    private EdgeFactory edgeFactory;
    private String edgeLabel;
    public createLink (EdgeFactory EdgeFactory, String EdgeLabel){
        edgeFactory = EdgeFactory;
        edgeLabel = EdgeLabel;
    }
    @Override
    public Edge map(Tuple3<Vertex, Vertex, Double> input) throws Exception {
        Edge edge = edgeFactory.createEdge(input.f0.getId(), input.f1.getId());
        edge.setProperty(edgeLabel, input.f2);
        return edge;
    }
}
