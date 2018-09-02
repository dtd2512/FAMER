package org.gradoop.famer.common.maxDeltaLinkSelection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.functions.*;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.findMax;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.makeEdgeWithSelectedStatus;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.swapF1F2;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class maxDeltaLinkSelection implements UnaryGraphToGraphOperator {
    private Double delta;
    public maxDeltaLinkSelection (Double Delta) { delta = Delta; }

    @Override
    public String getName() {
        return "maxDeltaLinkSelection";
    }

    @Override
    public LogicalGraph execute (LogicalGraph input) {
        DataSet<Tuple3<Edge, String, String>> link_srcId_trgtId = input.getEdges().map(new link2link_srcId_trgtId());
        DataSet<Tuple3<Edge, String, String>> link_trgtId_srcId = link_srcId_trgtId.map(new swapF1F2());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_srcId_trgtId.union(link_trgtId_srcId).groupBy(1).reduceGroup(new findMax(delta));
        DataSet<Edge> edges = edges_edgeId_isSelected.groupBy(1).reduceGroup(new makeEdgeWithSelectedStatus());


        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }
}
