package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.ConnectedComponents;
import org.gradoop.famer.common.FilterOutLinks.functions.FilterOutSpecificLinks;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class Recluster implements UnaryGraphToGraphOperator{
    private Integer edgeType;
    private Integer sourceNo;
    public Recluster (Integer EdgeStatus, Integer SourceNo){
        edgeType = EdgeStatus;
        sourceNo = SourceNo;
    }
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        DataSet<Edge> edges = input.getEdges().flatMap(new FilterOutSpecificLinks(edgeType));
        LogicalGraph temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
        temp = temp.callForGraph(new ConnectedComponents());
        temp = input.getConfig().getLogicalGraphFactory().fromDataSets(temp.getVertices(), input.getEdges());
        return temp.callForGraph(new DenotateClusters(sourceNo));
    }

    @Override
    public String getName() {
        return "Recluster";
    }
}
