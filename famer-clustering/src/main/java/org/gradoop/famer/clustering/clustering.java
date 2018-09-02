package org.gradoop.famer.clustering;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.*;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 *
 */
public class clustering implements UnaryGraphToGraphOperator{
    public enum ClusteringMethods {
        CONCOM, CORRELATION_CLUSTERING, CENTER, MERGE_CENTER, STAR1, STAR2
    };
    public enum ClusteringOutputType {
        Graph, GraphCollection
    };
    private String method;
    private LogicalGraph inputGraph;
    private Boolean isEdgeBidirection;
    private static clustering.ClusteringOutputType  clusteringOutputType;
    public clustering (LogicalGraph InputGraph, String ClusteringMethod, Boolean IsEdgeBidirection, String ClusteringOutputType){
        method = ClusteringMethod;
        isEdgeBidirection = IsEdgeBidirection;
        inputGraph = InputGraph;
        clusteringOutputType = clustering.ClusteringOutputType.valueOf(ClusteringOutputType);
//        config = inputGraph.getConfig();
    }
    @Override
    public LogicalGraph execute(LogicalGraph logicalGraph) {
        return null;
    }

    @Override
    public String getName() {
        return "clustering";
    }



    public LogicalGraph execute(){
        ClusteringMethods ClusteringMethod = ClusteringMethods.valueOf(method);
        LogicalGraph resultGraph = null;
        switch (ClusteringMethod){
            case CONCOM:
                resultGraph = inputGraph.callForGraph(new ConnectedComponents());
                break;
            case CORRELATION_CLUSTERING:
                resultGraph = inputGraph.callForGraph(new CorrelationClustering(isEdgeBidirection, clusteringOutputType));
                break;
            case CENTER:
                resultGraph = inputGraph.callForGraph(new Center(1, isEdgeBidirection, clusteringOutputType));
                break;
            case MERGE_CENTER:
                resultGraph = inputGraph.callForGraph(new MergeCenter(1, 0.0, isEdgeBidirection, clusteringOutputType));
                break;
            case STAR1:
                resultGraph = inputGraph.callForGraph(new Star(1, 1, isEdgeBidirection, clusteringOutputType));
                break;
            case STAR2:
                resultGraph = inputGraph.callForGraph(new Star(1, 2, isEdgeBidirection, clusteringOutputType));
                break;
        }
        return resultGraph;
    }
}
