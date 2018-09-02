package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class ComputeAP_Clstersize implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<Long, Long>>{
    private boolean intraSrcMatch;
    public ComputeAP_Clstersize (boolean isWithinDataSetMatch){
        intraSrcMatch = isWithinDataSetMatch;
    }
    public void reduce(Iterable<Tuple3<String, String, String>> in, Collector<Tuple2<Long, Long>> out) throws Exception {
        Long clstr_cnt = 0l;
        Long ap_cnt = 0l;
        if (!intraSrcMatch) {
            Collection<String> types = new ArrayList<String>();
            for (Tuple3<String, String, String> i : in) {
                clstr_cnt++;
                types.add(i.f2);
            }
            String[] typesArray = types.toArray(new String[types.size()]);
            for (int i = 0; i < typesArray.length; i++) {
                for (int j = i + 1; j < typesArray.length; j++) {
                    if (!typesArray[i].equals(typesArray[j]))
                        ap_cnt++;
                }
            }
        }
        else{
            for (Tuple3<String, String, String> i : in) {
                clstr_cnt++;
            }
            ap_cnt = ((clstr_cnt)*(clstr_cnt-1))/2;
        }
        out.collect(Tuple2.of(ap_cnt, clstr_cnt));
    }
}
