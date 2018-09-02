package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;


public class CreatePairedVertices implements GroupCombineFunction<Tuple5<Vertex, String, Long, Boolean, Integer>, Tuple2<Vertex, Vertex>> {
	
    private boolean intraDatasetComparison;
    private HashMap<String, HashSet<String>> graphPairs;
    
    public CreatePairedVertices(boolean intraDatasetComparison, HashMap<String, HashSet<String>> graphPairs){
    	this.intraDatasetComparison = intraDatasetComparison;
    	this.graphPairs = graphPairs;
    }
    
    @Override
    public void combine(Iterable<Tuple5<Vertex, String, Long, Boolean, Integer>> in, Collector<Tuple2<Vertex, Vertex>> out) throws Exception {
        Collection<Tuple2<Vertex,Boolean>> vertices = new ArrayList<>();
        for (Tuple5<Vertex, String, Long, Boolean, Integer> i:in){
            vertices.add(Tuple2.of(i.f0, i.f3));
        }
        Tuple2<Vertex, Boolean>[] verticesArray = vertices.toArray(new Tuple2[vertices.size()]);
        for (int i = 0; i< verticesArray.length && verticesArray [i].f1; i++){
            for (int j = i+1; j< verticesArray.length ; j++){
                if (intraDatasetComparison || !verticesArray[i].f0.getGraphIds().containsAny(verticesArray[j].f0.getGraphIds())) {
                	if(graphPairs.get("*") != null) {
                		out.collect(Tuple2.of(verticesArray[i].f0, verticesArray[j].f0));               		
                	}
                	else {
                    	Vertex v1 = verticesArray[i].f0;
                    	Vertex v2 = verticesArray[j].f0;
                    	
                    	String sourceGraph = v1.getPropertyValue("graphLabel").getString();
                    	String targetGraph = v2.getPropertyValue("graphLabel").getString();
                    	
                    	HashSet<String> allowedGraphs = graphPairs.get(sourceGraph);
                    	
                    	// allowedGraphs can be null if graph is not used for similarity measuring
                    	if(allowedGraphs != null && allowedGraphs.contains(targetGraph)) {
                    		out.collect(Tuple2.of(v1, v2));
                    	}
                    	else {
                    		// is going to be filtered out in the next step
                    		out.collect(Tuple2.of(v1, v1));
                    	}
                	}
                } 	
            }
        }
    }
}

