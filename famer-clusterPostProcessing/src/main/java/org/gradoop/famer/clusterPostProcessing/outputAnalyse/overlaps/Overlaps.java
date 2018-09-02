package org.gradoop.famer.clusterPostProcessing.outputAnalyse.overlaps;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.filterOverlappedVertices;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.overlaps.functions.clusterId2clusterId_length;
import org.gradoop.famer.common.functions.getF1Tuple2;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;


/**
 *
 */
public class Overlaps   {
    private DataSet<Vertex> clusteredGraphVertices;
    private DataSet<Edge> inputGraphEdges;
    public Overlaps(LogicalGraph ClusteredGraph){
        clusteredGraphVertices = ClusteredGraph.getVertices();
        inputGraphEdges = ClusteredGraph.getEdges();
    }

    public long getOverlappedVerticesNo () throws Exception {
        DataSet<Vertex> overlappedVertices = clusteredGraphVertices.flatMap(new filterOverlappedVertices());
        return overlappedVertices.count();
    }
    public long getOverlappedVerticesNo (Integer overlappedLength) throws Exception {
        DataSet<Vertex> overlappedVertices = clusteredGraphVertices.flatMap(new filterOverlappedVertices(overlappedLength));
        return overlappedVertices.count();
    }

    public DataSet<String> getOverlappedClusterIds(){
        DataSet<Vertex> vertices = clusteredGraphVertices.flatMap(new filterOverlappedVertices());
        return vertices.flatMap(new vertex2vertex_clusterId(true)).map(new getF1Tuple2()).distinct();
    }


    public DataSet<Tuple2<String, Integer>> getMaxOverlappedNo(){
        DataSet<String> clusterIds = clusteredGraphVertices.flatMap(new vertex2vertex_clusterId(false)).map(new getF1Tuple2());
        DataSet<Tuple2<String, Integer>> max = clusterIds.map(new clusterId2clusterId_length()).aggregate(Aggregations.MAX,1);
        return max;
    }




}

































