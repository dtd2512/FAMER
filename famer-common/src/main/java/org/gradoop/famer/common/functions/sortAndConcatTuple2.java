package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class sortAndConcatTuple2 implements MapFunction<Tuple2<String, String>, String> {
    @Override
    public String map(Tuple2<String, String> value) throws Exception {
        if (value.f0.compareTo(value.f1)<0)
            return value.f0+","+value.f1;
        else
            return value.f1+","+value.f0;
    }
}
