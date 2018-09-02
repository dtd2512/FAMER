package org.gradoop.famer.clusterPostProcessing.outputAnalyse.multiEntity1Src;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.multiEntity1Src.functions.multiEntity1SrcReducer;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 *
 */

public class multiEntity1Src  {
    private DataSet<Vertex> clusteredGraphVertices;
    private DataSet<Edge> inputGraphEdges;
    public multiEntity1Src(LogicalGraph ClusteredGraph){
        clusteredGraphVertices = ClusteredGraph.getVertices();
        inputGraphEdges = ClusteredGraph.getEdges();
    }

//    public multiEntity1Src(LogicalGraph ClusteredGraph, Integer SrcNo){
//        super(ClusteredGraph);
//        srcNo = SrcNo;
//    }
////    public long getWrongClusterNo () throws Exception {
////        return -1;
////    }
    public DataSet<String> getWrongClusterIds(){
        DataSet<Tuple2<Vertex, String>> vertex_clusterId = clusteredGraphVertices.flatMap(new vertex2vertex_clusterId(true));
        DataSet<String> wrongClusterIds = vertex_clusterId.groupBy(1).reduceGroup(new multiEntity1SrcReducer());
        return wrongClusterIds;
    }

}
