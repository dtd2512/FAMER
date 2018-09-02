package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve;

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
public class resolveBulkIteration implements UnaryGraphToGraphOperator {
    private Integer prioritizingMethod;
    public resolveBulkIteration (Integer PrioritizingMethod) {prioritizingMethod = PrioritizingMethod;}
    @Override
    public LogicalGraph execute(LogicalGraph input) {

        // preparation:

        // 1- assign componentId to vertices
        input = input.callForGraph(new ConnectedComponents());
        DataSet<Vertex> vertices = input.getVertices().map(new setComponentId());
        input = input.getConfig().getLogicalGraphFactory().fromDataSets(vertices, input.getEdges());


        // 2- each vertex is converted into a singleton
        vertices = input.getVertices().map(new setClusterId());
        input = input.getConfig().getLogicalGraphFactory().fromDataSets(vertices, input.getEdges());

        // 3- input graph to Cluster set (ClusterCollection)
        ClusterCollection cc = new ClusterCollection(input);
        DataSet<Cluster> clusters = cc.getClusterCollection();

        // 4- others
        DataSet<Tuple2<Cluster, Cluster>>  mergingClusters = null;



        /* start: bulk iteration */
        IterativeDataSet<Cluster> initial = clusters.iterate(Integer.MAX_VALUE);

        DataSet<Cluster> iteration = initial;
        if (prioritizingMethod == 1){
            // find the edge with the highest priority in each component
            DataSet<Tuple4<Cluster, Integer, Double, String>> cluster_degree_simVal_edgeId = iteration.flatMap(new cluster2cluster_degree_edgeId());
            DataSet<Tuple5<Cluster, Cluster, String, Integer, Double>> cluster_cluster_cmpntId_degree_simVal = cluster_degree_simVal_edgeId.groupBy(3).reduceGroup(new edge2ClusterPair());
            DataSet<Tuple5<Cluster, Cluster, String, Integer, Double>> firstPriority = cluster_cluster_cmpntId_degree_simVal.groupBy(2).reduceGroup(new find1stPriority());
            mergingClusters = firstPriority.map(new getPairs());
        }
        if (prioritizingMethod == 2){
            // find the edge with the highest priority in each component
            DataSet<Tuple5<Cluster, Integer, Double, Integer, String>> cluster_degree_simVal_strongness_edgeId = iteration.flatMap(new cluster2cluster_degree_strongness_edgeId());
            DataSet<Tuple4<Cluster, Cluster, String, Double>> cluster_cluster_cmpntId_priorityValue = cluster_degree_simVal_strongness_edgeId.groupBy(4).reduceGroup(new edge2ClusterPair2());
            mergingClusters = cluster_cluster_cmpntId_priorityValue.groupBy(2).maxBy(3).map(new getPairs2());
        }


        // remove conflicts from first priority
//        DataSet<Tuple3<Cluster, Cluster, String>> clusterPair_clusterId = firstPriority.flatMap(new clusterPair2clusterPair_clusterId());
//        DataSet<Tuple3<Cluster, Cluster, Boolean>> clusterPair_status = clusterPair_clusterId.groupBy(2).reduceGroup(new selectOne());
//        DataSet<Tuple2<Tuple2<Cluster, Cluster>, String>> remainedPairs = clusterPair_status.flatMap(new clusterPair2clusterPair_pairId(true)).distinct(1);
//        DataSet<Tuple2<Tuple2<Cluster, Cluster>, String>> deletedPairs = clusterPair_status.flatMap(new clusterPair2clusterPair_pairId(false)).distinct(1);
//        DataSet<Tuple2<Cluster, Cluster>>  mergingClusters = new minus().execute(remainedPairs, deletedPairs);


        // update clusters: step1: merge cluster at two ends of selected edges (high priority edges)
        DataSet<Cluster> mergedClusters = mergingClusters.map(new mergeClusters());
        DataSet<Tuple2<Cluster, String>> allClusters = iteration.map(new cluster2cluster_clusterId());
        DataSet<Tuple2<Cluster, String>> editedClusters = (mergingClusters.map(new getF0Tuple2()).union(mergingClusters.map(new getF1Tuple2()))).map(new cluster2cluster_clusterId());

        iteration = new minus().execute(allClusters, editedClusters).union(mergedClusters);

        // update clusters: step2: remove incorrect edges (edges that connect two incompatible clusters) +
        // integrate edges (if there is more than 1 edge in between 2 clusters, just integrate them to 1 edge)
        DataSet<Cluster> separatedClusters = iteration.flatMap(new getSeparatedComponents());
        DataSet<Tuple2<Cluster, String>> cluster_edgeId = iteration.flatMap(new cluster2cluster_interLinkId());
        DataSet<Tuple2<Cluster, Cluster>> clusterPairs = cluster_edgeId.groupBy(1).reduceGroup(new makeClusterPairs());
        DataSet<Tuple2<Cluster, String>> cluster_clusterId = clusterPairs.flatMap(new updateClusterInterLinks());
        iteration = cluster_clusterId.groupBy(1).reduceGroup(new uniformClusterInterlinks()).union(separatedClusters);

        // cluster_edge: termination check
        DataSet<Edge> interLinks = iteration.flatMap(new cluster2interLink());
        DataSet<Cluster> result = initial.closeWith(iteration, interLinks);

        /* end: bulk iteration */
        cc.setClusterCollection(result);
        return cc.toLogicalGraph(input.getConfig());
    }


    @Override
    public String getName() {
        return "resolveBulkIteration";
    }
}






















