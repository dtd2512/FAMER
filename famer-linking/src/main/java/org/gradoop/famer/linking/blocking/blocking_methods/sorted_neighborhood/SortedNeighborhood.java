package org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood;

import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.SortedNeighborhoodComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.func.AssignReducerId;
import org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.func.CreateLinkedVertices;
import org.gradoop.famer.linking.blocking.blocking_methods.sorted_neighborhood.func.Replicate;
import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.blocking.blocking_methods.cartesian_product.func.filterLoops;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Produces edges between vertices using the Sorted Neighborhood (SN) method of blocking.
 *
 * input: a LogicalGraph
 * output: a LogicalGraph with new generated edges
 */
public class SortedNeighborhood implements Serializable {

    private SortedNeighborhoodComponent sortedNeighborhoodComponent;
    public SortedNeighborhood(SortedNeighborhoodComponent SortedNeighborhoodComponent){
        sortedNeighborhoodComponent = SortedNeighborhoodComponent;
    }


    public DataSet<Tuple2<Vertex, Vertex>> execute (DataSet<Vertex> vertices, HashMap<String, HashSet<String>> graphPairs)  {

        DataSet<Tuple2<Vertex, String>> verticesWithAddKeydata = vertices.flatMap(sortedNeighborhoodComponent.getBlockingKeyGenerator());
//        int reducerNo = ExecutionEnvironment.getExecutionEnvironment().getParallelism();
        int reducerNo = ExecutionEnvironment.getExecutionEnvironment().getParallelism();
        verticesWithAddKeydata = verticesWithAddKeydata.map(new MapFunction<Tuple2<Vertex, String>, Tuple2<Vertex, String>>() {
            public Tuple2<Vertex, String> map(Tuple2<Vertex, String> value)  {
            	// saving as temporary variable to prevent run-time error
            	Vertex graphElement = value.f0;
            	return Tuple2.of(value.f0, value.f1 + graphElement.getGraphIds());
            }
        });

        DataSet<Tuple3<Vertex, String, Integer>> verticesKeyReducerId = verticesWithAddKeydata.flatMap(new AssignReducerId(reducerNo));

        DataSet<Tuple3<Vertex, String, Integer>> replicatedVerticesKeyReducerId = verticesKeyReducerId.groupBy(2).sortGroup(1, Order.ASCENDING)
                .reduceGroup(new Replicate(sortedNeighborhoodComponent.getWindowSize()));

        DataSet<Tuple2<Integer, Integer>> IdsWithPrev = verticesKeyReducerId.map(new MapFunction<Tuple3<Vertex, String, Integer>, Tuple1<Integer>>() {
            public Tuple1<Integer> map(Tuple3<Vertex, String, Integer> in) throws Exception {
                return Tuple1.of(in.f2);
            }
        }).distinct().
                map(new MapFunction<Tuple1<Integer>, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(Tuple1<Integer> in) {
                        return Tuple2.of(in.f0, "");
                    }
                }).groupBy(1).sortGroup(0, Order.ASCENDING).reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, Integer>>() {
            public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<Tuple2<Integer, Integer>> out) {
                Collection<Integer> ids = new ArrayList<Integer>();
                for (Tuple2<Integer, String> i : in) {
                    ids.add(i.f0);
                }
                Integer idsArray[] = ids.toArray(new Integer[ids.size()]);
                for (int i = 1; i <= idsArray.length - 1; i++) {
                    out.collect(Tuple2.of(idsArray[i], idsArray[i - 1]));
                }
            }
        });
        replicatedVerticesKeyReducerId = replicatedVerticesKeyReducerId.join(IdsWithPrev).where(2).equalTo(0).
                with(new JoinFunction<Tuple3<Vertex, String, Integer>, Tuple2<Integer, Integer>, Tuple3<Vertex, String, Integer>>() {
                    public Tuple3<Vertex, String, Integer> join(Tuple3<Vertex, String, Integer> in1, Tuple2<Integer, Integer> in2) {
                        return Tuple3.of(in1.f0, in1.f1, in2.f1);
                    }
                });

//        outputPairedVertices = replicatedVerticesKeyReducerId.union(verticesKeyReducerId).groupBy(2).sortGroup(1, Order.ASCENDING).
//                reduceGroup(new SortedNeighborhood(WindowSize, IntraDatasetComparison));


        DataSet<Tuple2<Vertex, Vertex>> vertexPairs = replicatedVerticesKeyReducerId
        		.union(verticesKeyReducerId)
        		.groupBy(2)
        		.sortGroup(1, Order.ASCENDING)
        		.reduceGroup(new CreateLinkedVertices(sortedNeighborhoodComponent.getWindowSize(),
        											  sortedNeighborhoodComponent.getIntraGraphComparison(),
        											  graphPairs));
        vertexPairs = vertexPairs.flatMap(new filterLoops());
        return vertexPairs;
    }
}
