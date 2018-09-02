package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class validateEdgeByClusterId implements MapFunction <Tuple3<Edge, String, String>, Edge>{
    @Override
    public Edge map(Tuple3<Edge, String, String> value) throws Exception {
        if (value.f1.equals(value.f2))
            value.f0.setProperty("IsCorrect", true);
        else
            value.f0.setProperty("IsCorrect", false);
        return value.f0;
    }
}
