package org.gradoop.famer.clusterPostProcessing.outputAnalyse.clusterAndNeighbors;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.clusterAndNeighbors.functions.filterEdgesJoin1;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.clusterAndNeighbors.functions.filterEdgesJoin2;
import org.gradoop.famer.common.functions.*;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.clusterAndNeighbors.functions.detectNeighborIds;

/**
 */
public class clusterAndNeighbors implements UnaryGraphToGraphOperator {
    private String clusterId;
    public clusterAndNeighbors (String ClusterId){
        clusterId = ClusterId;
    }
    @Override
    public String getName() {
        return "clusterAndNeighbors";
    }
    @Override
    public LogicalGraph execute (LogicalGraph ClusteredGraph){
        // find neighbor clusterIds
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = new link2link_srcVertex_trgtVertex(ClusteredGraph).execute();
        DataSet<Tuple2<String, String>> srcClusterId_trgtClusterId = edge_srcVertex_trgtVertex.map(new link2link_srcInfo_trgtInfo("ClusterId"))
                .map(new removeF0Tuple3());
        DataSet<Tuple1<String>> allClusterIds = srcClusterId_trgtClusterId.flatMap(new detectNeighborIds(clusterId)).distinct(0);

        // find vertices
        DataSet<Tuple2<Vertex, String>> vertex_clusterId = ClusteredGraph.getVertices().flatMap(new vertex2vertex_clusterId(true));
        DataSet<Tuple2<Vertex, String>> vertex_gradoopId = vertex_clusterId.join(allClusterIds).where(1).equalTo(0).with(new templateJoin1()).map(new vertex2vertex_gradoopId()).distinct(1);
        DataSet<Vertex> vertices = vertex_gradoopId.map(new getF0Tuple2());

        // find edges
        DataSet<Tuple3<Edge, String, String>> edge_srcId_trgtId = ClusteredGraph.getEdges().map(new link2link_srcId_trgtId());
        DataSet<Tuple1<String>> verticesGradoopIds = vertex_gradoopId.map(new getTuple1F1Tuple2());
        DataSet<Edge> edges = verticesGradoopIds.join(edge_srcId_trgtId).where(0).equalTo(1).with(new filterEdgesJoin1()).join(verticesGradoopIds).where(1).equalTo(0).with(new filterEdgesJoin2());

        return   ClusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
      
    }
}
