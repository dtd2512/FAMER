package org.gradoop.famer.linking.linking;

//import org.apache.commons.math.util.MultidimensionalCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.functions.removeF0Tuple3;
import org.gradoop.famer.common.util.functions.uniqVertexPairs;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.famer.linking.blocking.BlockMaker;
import org.gradoop.famer.linking.linking.func.addGraphlabel2Vertex;
import org.gradoop.famer.linking.linking.func.createLink;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.gradoop.famer.linking.linking.data_structures.LinkerComponent;
import org.gradoop.famer.linking.linking.func.head2label_gradoopId;
import org.gradoop.famer.linking.linking.func.vertex2vertex_graphGradoopId;
import org.gradoop.famer.linking.selection.Selector;
import org.gradoop.famer.linking.similarity_measuring.SimilarityMeasurer;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityFieldList;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;

import static org.apache.avro.SchemaBuilder.map;

/**
 */
public class Linker implements UnaryCollectionToCollectionOperator{
    private LinkerComponent linkerComponent;
    public Linker(LinkerComponent LinkerComponent){
        linkerComponent = LinkerComponent;
    }

    @Override
    public GraphCollection execute(GraphCollection input) {
        /************************PREPROCESSING******************************/
        /*******************************************************************/
//        DataSet<Tuple2<Vertex, GradoopId>> vertex_graphGradoopId = input.getVertices().map(new vertex2vertex_graphGradoopId());
//        DataSet<Tuple2<String, GradoopId>> graphLabel_gradoopId = input.getGraphHeads().map(new head2label_gradoopId());
//        DataSet<Vertex> vertices = vertex_graphGradoopId.join(graphLabel_gradoopId).where(1).equalTo(1).with(new addGraphlabel2Vertex());
    	DataSet<Vertex> vertices=input.getVertices();
        /************************BLOCKING***********************************/
        /*******************************************************************/
        DataSet<Tuple2<Vertex, Vertex>> blockedVertices = null;
        for (BlockingComponent blocking:linkerComponent.getBlockingComponents()){
            DataSet<Tuple2<Vertex, Vertex>> pairedVertices = new BlockMaker(blocking).execute(vertices, linkerComponent.getSimilarityComponents());
            if (blockedVertices == null)
                blockedVertices = pairedVertices;
            else
                blockedVertices = blockedVertices.union(pairedVertices);
        }
        if (linkerComponent.getKeepCurrentEdges() && linkerComponent.getRecomputeSimilarityForCurrentEdges()) {
            DataSet<Tuple2<Vertex, Vertex>> existantLinks = new link2link_srcVertex_trgtVertex(vertices, input.getEdges()).execute().map(new removeF0Tuple3());
            blockedVertices = blockedVertices.union(existantLinks);
        }
        
//        long initial = 0;
//        try { initial = blockedVertices.count();} catch (Exception e) {}
        
        blockedVertices = new uniqVertexPairs (blockedVertices).execute();

//        long filtered = 0;
//        try { filtered = blockedVertices.count();} catch (Exception e) {}
        
        /************************SIMILARITY MEASURING************************/
        /*******************************************************************/
        SimilarityMeasurer similarityMeasurer = new SimilarityMeasurer(linkerComponent.getSimilarityComponents());
        DataSet<Tuple3<Vertex, Vertex, SimilarityFieldList>> blockedVertices_similarityFields = blockedVertices.flatMap(similarityMeasurer);

        /************************SELECTION**********************************/
        /*******************************************************************/
        Selector selector = new Selector(linkerComponent.getSelectionComponent());
        DataSet<Tuple3<Vertex, Vertex, Double>> blockedVertices_similarityDegree = blockedVertices_similarityFields.flatMap(selector);

        /*************************POSTPROCESSING****************************/
        /*******************************************************************/
        DataSet<Edge> edges = blockedVertices_similarityDegree.map(new createLink(input.getConfig().getEdgeFactory(), linkerComponent.getEdgeLabel()));

        if (linkerComponent.getKeepCurrentEdges() && !linkerComponent.getRecomputeSimilarityForCurrentEdges())
            edges = edges.union(input.getEdges());

//        System.out.println("\n\n\n\n\n\n\n\n\ninitial: " + initial + ", filtered: " + filtered + "\n\n\n\n\n\n\n\n\n");
        
        return input.getConfig().getGraphCollectionFactory().fromDataSets(input.getGraphHeads(), vertices, edges);
    }

    @Override
    public String getName() {
        return Linker.class.getName();
    }
}
