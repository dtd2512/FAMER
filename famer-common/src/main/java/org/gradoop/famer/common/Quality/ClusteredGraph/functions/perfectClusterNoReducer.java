package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;

/** clusters with no overlaps
 */
public class perfectClusterNoReducer implements GroupReduceFunction <Tuple3<String, String, String>, Tuple2<String, Long>> {
    private String label;
    public perfectClusterNoReducer (String Label) { label = Label;}
    @Override
    public void reduce(Iterable<Tuple3<String, String, String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
        Collection<String> types = new ArrayList<>();
        for (Tuple3<String, String, String> value : values){
            if (types.contains(value.f2)) {
                out.collect(Tuple2.of(label,0l));
                return;
            }
            types.add(value.f2);
        }
        out.collect(Tuple2.of(label,1l));
    }
}
