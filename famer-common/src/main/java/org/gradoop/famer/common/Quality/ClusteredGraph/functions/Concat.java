package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by alieh on 2/10/17.
 */
public class Concat implements MapFunction <Tuple2<String, String>, Tuple1<String>> {
    public Tuple1<String> map(Tuple2<String, String> value) throws Exception {
        if(value.f0.compareTo(value.f1)<0)
            return Tuple1.of(value.f0 + "," + value.f1);
        else
            return Tuple1.of(value.f1 + "," + value.f0);
    }
}
