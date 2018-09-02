package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.computeOverlappedLength;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.filterOverlappedVertices;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.resolveReducer;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.updateOverlappedVertices;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.*;
import org.gradoop.famer.common.functions.getF0Tuple2;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.famer.common.functions.vertex2vertex_gradoopId;
import org.gradoop.famer.common.functions.vertex_T2vertexId_vertex_T;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 */
public class overlapResolve implements UnaryGraphToGraphOperator {
    @Override
    public String getName() {
        return "overlapResolve";
    }

    @Override
    public LogicalGraph execute (LogicalGraph clusteredGraph) {
        ClusterCollection clusterCollection = new ClusterCollection();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        while (true) {
            Integer currentOverlappedLength = 0;
            DataSet<Tuple1<Integer>> currentOverlappedLengthDS = clusteredGraph.getVertices().flatMap(new computeOverlappedLength()).aggregate(Aggregations.MIN, 0);
            try {
                for (Tuple1<Integer> col: currentOverlappedLengthDS.collect())
                    currentOverlappedLength = col.f0;
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (currentOverlappedLength == 0)
                break;
            System.out.println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::" +
                    ":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::" +
                    + currentOverlappedLength);

            // initialization
            DataSet<Cluster> clusters = null;
            try {
                clusters = clusterCollection.fromLogicalGraph(clusteredGraph);
            } catch (Exception e) {
                e.printStackTrace();
            }
            DataSet<Tuple2<Cluster, String>> cluster_clusterId = clusters.map(new cluster2cluster_clusterId());
            DataSet<Vertex> vertices = clusteredGraph.getVertices();

            // find vertices that must be resolved in this iteration
            DataSet<Vertex> toBeResolvedVertices = vertices.flatMap(new filterOverlappedVertices(currentOverlappedLength));

            // resolve
            DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
            DataSet<Tuple2<Vertex, Cluster>> toBeResolvedVertex_cluster = toBeResolvedVertex_clusterId.
                    join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin());
            DataSet<Vertex> updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
                    .groupBy(0).reduceGroup(new resolveReducer());

            // update vertex set
            DataSet<Tuple2<Vertex, String>> updatedVertex_gradoopId = updatedVertices.map(new vertex2vertex_gradoopId());
            /* remove repetitive vertices produced by cluster merging */
            updatedVertex_gradoopId = updatedVertex_gradoopId.groupBy(1).reduceGroup(new consolidateMergedClusterIds());
            DataSet<Tuple2<Vertex, String>> allVertex_gradoopId = vertices.map(new vertex2vertex_gradoopId());
            vertices = allVertex_gradoopId.leftOuterJoin(updatedVertex_gradoopId).where(1).equalTo(1).with(new updateOverlappedVertices()).map(new getF0Tuple2());
            clusteredGraph =  clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(vertices, clusteredGraph.getEdges());
        }

        return clusteredGraph;
    }
}
