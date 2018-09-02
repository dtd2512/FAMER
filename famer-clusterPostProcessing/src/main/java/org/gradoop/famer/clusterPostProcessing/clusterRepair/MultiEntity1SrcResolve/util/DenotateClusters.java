package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util;

import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class DenotateClusters implements UnaryGraphToGraphOperator{
    private Integer sourceNo;
    public DenotateClusters (Integer SourceNo){ sourceNo = SourceNo;}
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        ClusterCollection ClusterCollection = new ClusterCollection(input);
        ClusterCollection.DenotateClusterCollection(sourceNo);
        return ClusterCollection.toLogicalGraph(input.getConfig());
    }

    @Override
    public String getName() {
        return "IdentifyClusters";
    }
}
