package org.gradoop.famer.common.FilterOutLinks;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.FilterOutLinks.functions.FilterOutSpecificLinks;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class FilterOutLinks implements UnaryGraphToGraphOperator {
    private Integer edgeType;
    public FilterOutLinks (Integer EdgeType) { edgeType = EdgeType; }
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        DataSet<Edge> edges = input.getEdges().flatMap(new FilterOutSpecificLinks(edgeType));
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }

    @Override
    public String getName() {
        return "FilterOutLinks";
    }
}
