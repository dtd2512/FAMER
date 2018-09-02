package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by alieh on 2/9/17.
 */
public class GT2Tuple2 implements FlatMapFunction<String , Tuple2<String, String>> {
    private String gtsplitter;
    public GT2Tuple2 (String GTSplitter){
        gtsplitter = GTSplitter;
    }
    public void flatMap(String line, Collector<Tuple2<String, String>> out) {
        String[] split = line.split(gtsplitter);
        out.collect(Tuple2.of(split[0].replace("\"", ""), split[1].replace("\"", "")));
    }
}
