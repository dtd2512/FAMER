package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve;

import com.sun.xml.internal.ws.api.ha.StickyFeature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions.*;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions2.cluster2cluster_degree_strongness_edgeId;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions2.edge2ClusterPair2;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions2.getPairs2;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.ConnectedComponents;
import org.gradoop.famer.common.functions.getF0Tuple2;
import org.gradoop.famer.common.functions.getF1Tuple2;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
import org.gradoop.famer.common.util.minus;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.MAX;

/**
 */
public class ParallelResolve implements UnaryGraphToGraphOperator {
    private Integer prioritizingMethod;
    public ParallelResolve (Integer PrioritizingMethod) {prioritizingMethod = PrioritizingMethod;}
    @Override
    public LogicalGraph execute(LogicalGraph input) {


        // assign priorities to edges
        // call message passing algo

       return null;
    }


    @Override
    public String getName() {
        return "resolveBulkIteration";
    }
}






















