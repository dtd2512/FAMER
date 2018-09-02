package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
public class enumeratedEntities2Link implements FlatMapFunction <Tuple3<String, Integer,Integer>, Tuple2<String, Boolean>>{
    @Override
    public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, Boolean>> out) throws Exception {
        if (value.f1 > 0 && value.f2 > 0)
            out.collect(Tuple2.of(value.f0, true));
        else if (value.f1 > 0 && value.f2 == 0)
            out.collect(Tuple2.of(value.f0, false));
    }
}
