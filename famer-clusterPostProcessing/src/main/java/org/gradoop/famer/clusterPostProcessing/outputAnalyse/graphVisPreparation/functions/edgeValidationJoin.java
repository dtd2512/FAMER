package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class edgeValidationJoin implements JoinFunction <Tuple2<String, Boolean>, Tuple2<Edge, String>, Edge> {
    @Override
    public Edge join(Tuple2<String, Boolean> first, Tuple2<Edge, String> second) throws Exception {
        second.f0.setProperty("IsCorrect", first.f1);
        return second.f0;
    }
}
