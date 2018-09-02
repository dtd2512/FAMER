package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.*;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.*;
import org.gradoop.famer.common.functions.*;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 */
public class overlapResolveBulkIteration implements UnaryGraphToGraphOperator {
    @Override
    public String getName() {
        return "overlapResolveBulkIteration";
    }
    private Integer iteration;
    public overlapResolveBulkIteration(Integer iterationNo){iteration = iterationNo;}

    @Override
    public LogicalGraph execute (LogicalGraph clusteredGraph) {

        ClusterCollection cc = new ClusterCollection(clusteredGraph);
        DataSet<Cluster> clusters = cc.getClusterCollection();


        IterativeDataSet<Cluster> initial = clusters.iterate(Integer.MAX_VALUE);
        DataSet<Cluster> iteration = initial;


        /* start: bulk iteration */
        DataSet<Tuple2<Cluster, String>> cluster_clusterId = iteration.map(new cluster2cluster_clusterId());
        DataSet<Vertex> vertices = iteration.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());


        // find vertices that must be resolved in this iteration
        DataSet<Tuple2<Vertex, Integer>> vertex_overlapLength = vertices.flatMap(new overlapLength());
        DataSet<Vertex> toBeResolvedVertices = vertex_overlapLength.minBy(1).join(vertex_overlapLength).where(1).equalTo(1).with(new joinGetSecond()).map(new getF0Tuple2());

        // resolve
        DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
        DataSet<Tuple2<Vertex, Cluster>> toBeResolvedVertex_cluster = toBeResolvedVertex_clusterId.
                    join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin());

        DataSet<Vertex> updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
                    .groupBy(0).reduceGroup(new resolveReducer2());

        // update vertex set
        DataSet<Tuple2<Vertex, String>> updatedVertex_gradoopId = updatedVertices.map(new vertex2vertex_gradoopId());

        /* remove repetitive vertices produced by cluster merging */
        updatedVertex_gradoopId = updatedVertex_gradoopId.groupBy(1).reduceGroup(new consolidateMergedClusterIds());
        DataSet<Tuple2<Vertex, String>> allVertex_gradoopId = vertices.map(new vertex2vertex_gradoopId());
        vertices = allVertex_gradoopId.leftOuterJoin(updatedVertex_gradoopId).where(1).equalTo(1).with(new updateOverlappedVertices()).map(new getF0Tuple2());
        clusteredGraph =  clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(vertices, clusteredGraph.getEdges());

        cc.setClusterCollection(clusteredGraph);
        iteration = cc.getClusterCollection();


        DataSet<Vertex> remainingToBeResolvedVertices = vertices.flatMap(new filterOverlappedVertices());
        DataSet<Cluster> result = initial.closeWith(iteration, remainingToBeResolvedVertices);

        cc.setClusterCollection(result);
        return cc.toLogicalGraph(clusteredGraph.getConfig());
    }
}





















