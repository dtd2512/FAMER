
package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

        import org.apache.flink.api.common.functions.GroupReduceFunction;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.api.java.tuple.Tuple3;
        import org.apache.flink.util.Collector;

        import java.util.ArrayList;
        import java.util.Collection;

/** clusters with no overlaps
 */
public class perfectCompleteClusterNoReducer implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, Long>> {
    private String label;
    private Integer srcNo;
    public perfectCompleteClusterNoReducer (String Label, Integer SrcNo) { label = Label; srcNo = SrcNo;}
    @Override
    public void reduce(Iterable<Tuple3<String, String, String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
        Collection<String> types = new ArrayList<>();
        for (Tuple3<String, String, String> value : values){
            if (types.contains(value.f2)) {
                return;
            }
            types.add(value.f2);
        }
        if (types.size() == srcNo)
            out.collect(Tuple2.of(label,1l));
    }
}

