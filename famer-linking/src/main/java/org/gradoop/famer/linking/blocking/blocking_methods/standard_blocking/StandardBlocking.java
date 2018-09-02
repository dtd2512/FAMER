package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking;

import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.StandardBlockingComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func.*;
import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.blocking.blocking_methods.cartesian_product.func.filterLoops;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;


public class StandardBlocking implements Serializable {
    private StandardBlockingComponent standardBlockingComponent;
    public StandardBlocking(StandardBlockingComponent StandardBlockingComponent) {
        standardBlockingComponent = StandardBlockingComponent;
    }

    public DataSet<Tuple2<Vertex, Vertex>> execute (DataSet<Vertex> vertices, HashMap<String, HashSet<String>> graphPairs)  {

        DataSet<Tuple2<Vertex, String>> vertex_key = vertices.flatMap(standardBlockingComponent.getBlockingKeyGenerator());
        DataSet<Tuple3<Vertex, String, Integer>> vertex_key_partitionId = vertex_key.map(new DiscoverPartitionId());
        DataSet<Tuple3<String, Integer, Long>> key_partitionId_no = vertex_key_partitionId.groupBy(2,1).combineGroup(new EnumeratePartitionEntities());
        UnsortedGrouping<Tuple3<String, Integer, Long>> key_partitionId_no_GroupedByKey = key_partitionId_no.groupBy(0);

        /* Generate vertex index */
        DataSet<Tuple3<String, Integer, Long>> key_partitionId_startPoint = key_partitionId_no_GroupedByKey.sortGroup(1, Order.ASCENDING).reduceGroup(new ComputePartitionEnumerationStartPoint());
        DataSet<Tuple4<Vertex, String, Integer, Long>> vertex_key_partitionid_startPoit = key_partitionId_startPoint.join(vertex_key_partitionId).where(0,1).equalTo(1,2).with(new ConcatVertextoPartitionInfo());
        DataSet<Tuple3<Vertex, String, Long>> vertex_key_vertexId = vertex_key_partitionid_startPoit.groupBy(1,2).reduceGroup(new AssignVertexIndex());

        /* Prepare key (block) information (size, index, no. of pairs in prev blocks, no. of all pairs) */
        DataSet <Tuple3<String, Long, Long>> key_size_index = DataSetUtils.zipWithUniqueId(key_partitionId_no_GroupedByKey.reduceGroup(new ComputeBlockSize())).map(new AssignBlockIndex());
        DataSet<Tuple5<String, Long, Long, Long, Long>> key_size_index_prevBlocksPairs_allPairs = key_size_index.reduceGroup(new ComputePrevBlocksPairNo_AllPairs());

        /* Provide information (BlockIndex, BlockSize, PrevBlockPairs, allPairs) for each vertex         */
        DataSet<Tuple6<Vertex, String, Long, Long, Long, Long>> vertex_key_VIndex_blockSize_prevBlocksPairs_allPairs = vertex_key_vertexId.join(key_size_index_prevBlocksPairs_allPairs).where(1).equalTo(0).with(new ConcatAllInfoToVertex());

        /* Load Balancing */
        Integer ReducerNo = standardBlockingComponent.getParallelismDegree();
        DataSet<Tuple5<Vertex, String, Long, Boolean, Integer>> vertex_key_VIndex_isLast_reducerId = vertex_key_VIndex_blockSize_prevBlocksPairs_allPairs.flatMap(new replicateAndAssignReducerId(ReducerNo));
        vertex_key_VIndex_isLast_reducerId = vertex_key_VIndex_isLast_reducerId.partitionCustom(new PartitionVertices(), 4);

        /* Make pairs */
        DataSet<Tuple2<Vertex, Vertex>> PairedVertices = vertex_key_VIndex_isLast_reducerId
        		.groupBy(1)
        		.sortGroup(2, Order.ASCENDING)
        		.combineGroup(new CreatePairedVertices(
        				standardBlockingComponent.getIntraGraphComparison(),
        				graphPairs));
        PairedVertices = PairedVertices.flatMap(new filterLoops());
        return PairedVertices;
    }
}





























