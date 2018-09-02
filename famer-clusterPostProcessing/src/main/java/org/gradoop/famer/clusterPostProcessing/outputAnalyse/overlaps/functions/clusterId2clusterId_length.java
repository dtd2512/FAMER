package org.gradoop.famer.clusterPostProcessing.outputAnalyse.overlaps.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class clusterId2clusterId_length implements MapFunction<String, Tuple2<String, Integer>>{
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return Tuple2.of(value, value.split(",").length);
    }
}
