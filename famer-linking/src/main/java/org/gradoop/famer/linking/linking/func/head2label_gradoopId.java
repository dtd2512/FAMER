package org.gradoop.famer.linking.linking.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 */
public class head2label_gradoopId implements MapFunction <GraphHead, Tuple2<String, GradoopId>>{
    @Override
    public Tuple2<String, GradoopId> map(GraphHead graphHead) throws Exception {
        return Tuple2.of(graphHead.getLabel(), graphHead.getId());
    }
}
