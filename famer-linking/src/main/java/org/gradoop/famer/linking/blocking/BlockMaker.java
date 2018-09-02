package org.gradoop.famer.linking.blocking;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.blocking.blocking_methods.cartesian_product.CartesianProduct;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.*;
import org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.SortedNeighborhood;
import org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.StandardBlocking;
import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;



/**
 *
 * input: DataSet<Vertex>
 * output: DataSet<Tuple2<Vertex,Vertex>>
 */
public class BlockMaker implements Serializable {
    BlockingComponent blockingComponent;
    public BlockMaker(BlockingComponent BlockingComponent) {
        blockingComponent = BlockingComponent;
    }

    public DataSet<Tuple2<Vertex,Vertex>> execute(DataSet<Vertex> vertices, Collection<SimilarityComponent> similarityComponents)  {
    	
    	HashMap<String, HashSet<String>> graphPairs = getGraphPairs(similarityComponents);
    	
        BlockingMethod blockingMethod = blockingComponent.getBlockingMethod();
        switch (blockingMethod){
            case CARTESIAN_PRODUCT:
                return new CartesianProduct((CartesianProductComponent) blockingComponent).execute(vertices, graphPairs);
            case STANDARD_BLOCKING:
                return new StandardBlocking((StandardBlockingComponent) blockingComponent).execute(vertices, graphPairs);
            case SORTED_NEIGHBORHOOD:
                return new SortedNeighborhood((SortedNeighborhoodComponent) blockingComponent).execute(vertices, graphPairs);
        }
        return null;
    }
    
    private static HashMap<String, HashSet<String>> getGraphPairs(Collection<SimilarityComponent> similarityComponents){
    	
    	HashMap<String, HashSet<String>> result = new HashMap<String, HashSet<String>>();
    	
    	for(SimilarityComponent similarityComponent: similarityComponents) {
    		
    		String sourceGraph = similarityComponent.getSrcGraphLabel();
    		String targetGraph = similarityComponent.getTargetGraphLabel();
    		
    		if(result.containsKey(sourceGraph)) {
    			result.get(sourceGraph).add(targetGraph);
    		}
    		else {
    			HashSet<String> targetGraphs = new HashSet<String>();
    			targetGraphs.add(targetGraph);
    			result.put(sourceGraph, targetGraphs);
    		}
    	}
    	
    	// swapping source and target to get results for reverse direction
    	for(SimilarityComponent similarityComponent: similarityComponents) {
    		
    		String targetGraph = similarityComponent.getSrcGraphLabel();
    		String sourceGraph = similarityComponent.getTargetGraphLabel();
    		
    		if(result.containsKey(sourceGraph)) {
    			result.get(sourceGraph).add(targetGraph);
    		}
    		else {
    			HashSet<String> targetGraphs = new HashSet<String>();
    			targetGraphs.add(targetGraph);
    			result.put(sourceGraph, targetGraphs);
    		}
    	}
    	
    	return result;
    	
    }
}

