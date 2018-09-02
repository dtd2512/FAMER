package org.gradoop.famer.clusterPostProcessing.clusterRepair.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */
public class getOverlapLength implements MapFunction <Vertex, Tuple1<Integer>> {

    @Override
    public Tuple1<Integer> map(Vertex in) throws Exception {
        return Tuple1.of(in.getPropertyValue("ClusterId").toString().split(",").length);
    }
}
