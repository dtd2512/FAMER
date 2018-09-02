package org.gradoop.famer.linking.blocking.blocking_methods.cartesian_product;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.blocking.blocking_methods.cartesian_product.func.filterIntraPairs;
import org.gradoop.famer.linking.blocking.blocking_methods.cartesian_product.func.filterLoops;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.CartesianProductComponent;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * input: DataSet<Vertex>
 * output: DataSet<Tuple2<Vertex,Vertex>>
 */
public class CartesianProduct implements Serializable {

    private CartesianProductComponent blockingComponent;
    public CartesianProduct(CartesianProductComponent CartesianProductComponent) {
        blockingComponent = CartesianProductComponent ;
    }

    public DataSet<Tuple2<Vertex, Vertex>> execute(DataSet<Vertex> vertices, HashMap<String, HashSet<String>> graphPairs)  {
        DataSet<Tuple2<Vertex, Vertex>> crossed = vertices.cross(vertices).with(new CrossFunction<Vertex, Vertex, Tuple2<Vertex, Vertex>>() {
            public Tuple2<Vertex, Vertex> cross(Vertex v1, Vertex v2) {
                
            	if(graphPairs.get("*") != null) {
            		return Tuple2.of(v1, v2);
            	}
            	else {
                	String sourceGraph = v1.getPropertyValue("graphLabel").getString();
                	String targetGraph = v2.getPropertyValue("graphLabel").getString();
                	
                	HashSet<String> allowedGraphs = graphPairs.get(sourceGraph);
                	
                	// allowedGraphs can be null if graph is not used for similarity measuring
                	if(allowedGraphs != null && allowedGraphs.contains(targetGraph)) {
                		return Tuple2.of(v1, v2);
                	}
                	else {
                		// is going to be filtered out in the next step
                		return Tuple2.of(v1, v1);
                	}
            	}
            }
        });
        crossed = crossed.flatMap(new filterLoops());
    //    if (!blockingComponent.getIntraGraphComparison())
       //     crossed = crossed.flatMap(new filterIntraPairs());
        return crossed;
    }
}