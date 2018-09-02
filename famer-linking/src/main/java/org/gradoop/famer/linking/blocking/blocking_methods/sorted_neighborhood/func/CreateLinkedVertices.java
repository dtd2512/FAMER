package org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.func;


        import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.GroupReduceFunction;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.api.java.tuple.Tuple3;
        import org.apache.flink.util.Collector;
        import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Transformation GroupReduceFunction function to generate edges between all vertices inside a window

 * input: a groups of vertices
 * output: edges between all vertices of the group
 */
public class CreateLinkedVertices implements GroupReduceFunction<Tuple3<Vertex, String, Integer>, Tuple2<Vertex, Vertex>> {
    private Integer WindowSize;
    private boolean IntraDatasetComparison;
    private HashMap<String, HashSet<String>> graphPairs;

    public CreateLinkedVertices(Integer windowSize, boolean intraDatasetComparison, HashMap<String, HashSet<String>> graphPairs) {
        WindowSize = windowSize;
        IntraDatasetComparison = intraDatasetComparison;
        this.graphPairs = graphPairs;
    }
    
    public void reduce(Iterable<Tuple3<Vertex, String, Integer>> in, Collector<Tuple2<Vertex, Vertex>> out) {
        Vertex verticesArray[] = new Vertex[WindowSize];
        int cnt=0;
        int windowNo=0;
        boolean fillWindowphase= true;
        int newElemIndex = 0;
        for (Tuple3<Vertex, String, Integer> elem : in) {
            if (fillWindowphase) {
                if(windowNo==0) {
                    verticesArray[cnt] = elem.f0;
                    cnt++;
                    if(cnt==WindowSize)
                        fillWindowphase = false;
                }
                else {
                    newElemIndex = (windowNo%WindowSize)-1;
                    if (newElemIndex<0)
                        newElemIndex = WindowSize-1;
                    verticesArray[newElemIndex] = elem.f0;
                    fillWindowphase = false;
                }

            }
            if (!fillWindowphase)   {
                if (windowNo==0) {
                    for (int i = 0; i < WindowSize; i++) {
                        for (int j = i + 1; j < WindowSize; j++) {
                            if (!verticesArray[i].getId().equals(verticesArray[j].getId())) {
                                if (IntraDatasetComparison ||
                                        (!IntraDatasetComparison && !verticesArray[i].getGraphIds().containsAny(verticesArray[j].getGraphIds())))
                                {
                                	if(graphPairs.get("*") != null) {
                                    	out.collect(Tuple2.of(verticesArray[i],verticesArray[j] ));                               		
                                	}
                                	else {
                                    	Vertex v1 = verticesArray[i];
                                    	Vertex v2 = verticesArray[j];
                                    	
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
                else {
                    for (int i = 0; i < WindowSize; i++) {
                        if (!verticesArray[i].getId().equals(verticesArray[newElemIndex].getId())) {
                            if (IntraDatasetComparison || (!IntraDatasetComparison && !verticesArray[i].getGraphIds().containsAny(verticesArray[newElemIndex].getGraphIds())))
                            {
                            	if(graphPairs.get("*") != null) {
                                	out.collect(Tuple2.of(verticesArray[i],verticesArray[newElemIndex]));                            		
                            	}
                            	else {
                                	Vertex v1 = verticesArray[i];
                                	Vertex v2 = verticesArray[newElemIndex];
                                	
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
                windowNo++;
                fillWindowphase = true;
            }
        }
        if (cnt < WindowSize){
            for (int i = 0; i < cnt; i++) {
                for (int j = i + 1; j < cnt; j++) {
                    if (!verticesArray[i].getId().equals(verticesArray[j].getId())) {
                        if (IntraDatasetComparison || (!IntraDatasetComparison && !verticesArray[i].getGraphIds().containsAny(verticesArray[j].getGraphIds())))
                        {
                        	if(graphPairs.get("*") != null) {
                            	out.collect(Tuple2.of(verticesArray[i], verticesArray[j]));                        		
                        	}
                        	else {
                            	Vertex v1 = verticesArray[i];
                            	Vertex v2 = verticesArray[j];
                            	
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
    }
}
