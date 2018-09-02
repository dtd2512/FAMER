package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class ppcReducer implements Serializable, GroupReduceFunction <Tuple3<String, String, String>, Tuple2<Long, Long>>{
    private Integer srcNo;
    public ppcReducer (Integer SourceNo){srcNo = SourceNo;}

    public void reduce(Iterable<Tuple3<String, String, String>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
        Collection<String> types = new ArrayList<String>();
        boolean isPerfect = true;
        for (Tuple3<String, String, String> i : values) {
            if (types.contains(i.f2)) {
                isPerfect = false;
                break;
            }
            types.add(i.f2);
        }
        if (!isPerfect)
            out.collect(Tuple2.of(0l, 0l));
        else {
            if (types.size() == srcNo)
                out.collect(Tuple2.of(1l, 1l));
            else
                out.collect(Tuple2.of(1l, 0l));
        }

    }
}
