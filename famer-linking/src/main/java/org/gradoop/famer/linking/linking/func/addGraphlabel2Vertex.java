package org.gradoop.famer.linking.linking.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class addGraphlabel2Vertex implements JoinFunction<Tuple2<Vertex, GradoopId>, Tuple2<String, GradoopId>, Vertex>{
    @Override
    public Vertex join(Tuple2<Vertex, GradoopId> first, Tuple2<String, GradoopId> second) throws Exception {
        first.f0.setProperty("graphLabel", second.f0);
        return first.f0;
    }
}

