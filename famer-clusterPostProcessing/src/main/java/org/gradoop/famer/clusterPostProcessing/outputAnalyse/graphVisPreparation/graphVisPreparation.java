package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions.*;
import org.gradoop.famer.common.functions.sortAndConcatTuple2;
import org.gradoop.famer.common.functions.sortAndConcatTuple3;
import org.gradoop.famer.common.maxDeltaLinkSelection.maxDeltaLinkSelection;
import org.gradoop.famer.common.maxDeltaLinkSelection.maxLinkSrcSelection;
import org.gradoop.famer.common.util.EnumerateEntitiesOf2Sets;
import org.gradoop.famer.common.functions.getF1Tuple2;
import org.gradoop.famer.common.functions.link2link_srcInfo_trgtInfo;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.Collection;

/**
 *
 */
public class graphVisPreparation implements UnaryGraphToGraphOperator {
    private DataSet<Tuple2<String, String>> PMTuples;
    private String linkValidationPropertyTitle;
    private Collection <Tuple3<String, Integer, Integer>> vertexVis_LabelPropertyCollection;
    private Double delta;
    public graphVisPreparation(DataSet<Tuple2<String, String>> PerfectMatchTuples, String LinkValidationPropertyTitle,
                               Collection <Tuple3<String, Integer, Integer>> VertexVis_LabelPropertyCollection, Double Delta){
        PMTuples = PerfectMatchTuples;
        linkValidationPropertyTitle = LinkValidationPropertyTitle;
        vertexVis_LabelPropertyCollection = VertexVis_LabelPropertyCollection;
        delta = Delta;
    }
    @Override
    public String getName() {
        return "graphVisPreparation";
    }
    @Override
    public LogicalGraph execute(LogicalGraph ClusteredGraph) {
        // validate links
        if (PMTuples == null)
            ClusteredGraph = validateLinksWithEmbeddedPMClusterId (ClusteredGraph);
        else
            ClusteredGraph = validateLinksWithPerfectMatch (ClusteredGraph);

        // add vis_label to vertices and edges
        ClusteredGraph = addVis_Label(ClusteredGraph);
        // select max link
//        ClusteredGraph = selectLinks(ClusteredGraph);
        // decorate links
        ClusteredGraph = addLinkAttributes (ClusteredGraph);
        return ClusteredGraph;
    }



    private LogicalGraph validateLinksWithPerfectMatch (LogicalGraph input){
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = new link2link_srcVertex_trgtVertex(input).execute();
        DataSet<Tuple3<Edge, String, String>> edge_srcRecId_trgtRecId = edge_srcVertex_trgtVertex.map(new link2link_srcInfo_trgtInfo(linkValidationPropertyTitle));
        DataSet<Tuple2<Edge, String>> edge_srcRecIdTrgtRecId = edge_srcRecId_trgtRecId.map(new sortAndConcatTuple3());
        DataSet<String> srcRecIdTrgtRecId = edge_srcRecIdTrgtRecId.map(new getF1Tuple2());
        DataSet<String> PMSrcTrgt = PMTuples.map(new sortAndConcatTuple2());
        DataSet<Tuple3<String, Integer,Integer>> enumerateEntitiesOf2Sets = new EnumerateEntitiesOf2Sets(srcRecIdTrgtRecId, PMSrcTrgt).execute();
        DataSet<Tuple2<String, Boolean>> srcRecIdTrgtRecId_isValid = enumerateEntitiesOf2Sets.flatMap(new enumeratedEntities2Link());
        DataSet<Edge> edges = srcRecIdTrgtRecId_isValid.join(edge_srcRecIdTrgtRecId).where(0).equalTo(1).with(new edgeValidationJoin());

        input = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
        return input;
    }

    private LogicalGraph validateLinksWithEmbeddedPMClusterId (LogicalGraph input){
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = new link2link_srcVertex_trgtVertex(input).execute();
        DataSet<Tuple3<Edge, String, String>> edge_srcPMClusterId_trgtPMClusterId = edge_srcVertex_trgtVertex.map(new link2link_srcInfo_trgtInfo(linkValidationPropertyTitle));
        DataSet<Edge> edges = edge_srcPMClusterId_trgtPMClusterId.map(new validateEdgeByClusterId());
        input = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
        return input;
    }
    private LogicalGraph addVis_Label (LogicalGraph input){
        DataSet<Vertex> vertices = input.getVertices().map(new addVertexVis_Label(vertexVis_LabelPropertyCollection));
        DataSet<Edge> edges = input.getEdges().map(new addLinkVis_Label("value"));

        return input.getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
    }
    private LogicalGraph selectLinks (LogicalGraph input){
        return input.callForGraph(new maxLinkSrcSelection(delta));
    }
    private LogicalGraph addLinkAttributes (LogicalGraph input) {
        DataSet<Edge> edges = input.getEdges().map(new decorateLinks());
        input = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
        return input;
    }

}
