package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;


@FunctionAnnotation.ForwardedFields({"f0","f1"})

public final class DiscoverPartitionId extends RichMapFunction<Tuple2<Vertex, String>, Tuple3<Vertex, String, Integer>> {

    public Tuple3<Vertex,String,Integer> map(Tuple2<Vertex,String> in) throws Exception {
        int pId = getRuntimeContext().getIndexOfThisSubtask();
        return Tuple3.of(in.f0, in.f1,pId);
    }
}
