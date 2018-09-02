package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.DenotateClusters;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.Recluster;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.resolveBulkIteration;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.filterOutLink;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.removeLinks2PerfectClusters;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.ConnectedComponents;
import org.gradoop.famer.common.FilterOutLinks.functions.FilterOutSpecificLinks;
import org.gradoop.famer.common.functions.filterOutVertexByPropertyValue;
import org.gradoop.famer.common.functions.vertex2vertex_gradoopId;
import org.gradoop.famer.common.maxDeltaLinkSelection.maxLinkSrcSelection;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.famer.common.util.minus;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class multiEntity1SrcResolve implements UnaryGraphToGraphOperator {
    public enum multiEntity1SrcResolveType{SEQUENTIAL, PARALLEL}

    private Double delta;
    private Integer phase;
    private Integer sourceNo;
    private multiEntity1SrcResolveType type;
    @Override
    public String getName() {
        return "multiEntity1SrcResolve";
    }
    public multiEntity1SrcResolve (Double Delta, Integer Phase, Integer SourceNo, multiEntity1SrcResolveType Type){delta=Delta;phase= Phase; sourceNo = SourceNo;type=Type;}
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        LogicalGraph clusteredGraph = input;
        switch (phase) {

            // Phase 1: split clusters by keeping just strong links
            case 1:
                clusteredGraph = clusteredGraph.callForGraph(new maxLinkSrcSelection(delta));
                clusteredGraph = clusteredGraph.callForGraph(new Recluster(1, sourceNo));
                clusteredGraph = clusteredGraph.transformVertices((current, transformed) -> {
                    String ClusterId = current.getPropertyValue("ClusterId").toString();
                    transformed.setProperties(current.getProperties());
                    transformed.setProperty("ClusterId", ClusterId);
                    return transformed;
                });

//                break;

            // Phase 2: merging  !(perfect complete) clusters + split (perfect complete) clusters + remove weak links
            case 2:
                DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_trgtVertex = new link2link_srcVertex_trgtVertex(clusteredGraph).execute();
                DataSet<Edge> edges = link_srcVertex_trgtVertex.flatMap(new removeLinks2PerfectClusters());
                edges = edges.flatMap(new FilterOutSpecificLinks(0));
                clusteredGraph = clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(clusteredGraph.getVertices(), edges);
                clusteredGraph = clusteredGraph.callForGraph(new ConnectedComponents());
                clusteredGraph = clusteredGraph.transformVertices((current, transformed) -> {
                    String ClusterId = current.getPropertyValue("ClusterId").toString();
                    transformed.setProperties(current.getProperties());
                    transformed.setProperty("ClusterId", ClusterId);
                    return transformed;
                });
                clusteredGraph = clusteredGraph.callForGraph(new DenotateClusters(sourceNo));
//                break;


            // Phase 3: resolve bad clusters (clusters with more than one entity from one src)
//            case 3:
                // separate vertices and edges of perfect complete clusters//isCompletePerfect
                Collection<String> removing = new ArrayList<>();
                removing.add("isPerfect");
                DataSet<Vertex> clusteredVertices = clusteredGraph.getVertices().flatMap(new filterOutVertexByPropertyValue(removing));
                DataSet<Tuple2<Vertex,String>> allVertices_ids = clusteredGraph.getVertices().map(new vertex2vertex_gradoopId());
                DataSet<Tuple2<Vertex,String>> clusteredVertices_ids = clusteredVertices.map(new vertex2vertex_gradoopId());
                DataSet<Vertex> remainingVertices = new minus().execute(allVertices_ids, clusteredVertices_ids);

                DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_trgtVertex_2 = new link2link_srcVertex_trgtVertex(clusteredGraph).execute();
                DataSet<Edge> remainingLinks = link_srcVertex_trgtVertex_2.flatMap(new filterOutLink(removing));


                // The resolve algorithm
                LogicalGraph remainingGraph = clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(remainingVertices, remainingLinks);
                if (type.equals(multiEntity1SrcResolveType.SEQUENTIAL))
                    remainingGraph = remainingGraph.callForGraph(new resolveBulkIteration(2));
                else if (type.equals(multiEntity1SrcResolveType.PARALLEL))
                    remainingGraph = remainingGraph.callForGraph(new resolveBulkIteration(2));

                clusteredGraph = clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(remainingGraph.getVertices().union(clusteredVertices),
                input.getEdges());
        }
        return clusteredGraph;
    }


}



























