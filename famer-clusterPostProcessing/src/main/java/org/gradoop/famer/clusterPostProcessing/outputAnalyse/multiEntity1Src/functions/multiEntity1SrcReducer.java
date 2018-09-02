package org.gradoop.famer.clusterPostProcessing.outputAnalyse.multiEntity1Src.functions;


import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class multiEntity1SrcReducer implements GroupReduceFunction<Tuple2<Vertex, String>, String> {
    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> values, Collector<String> out) throws Exception {
        Collection<String> sources = new ArrayList<>();
        for (Tuple2<Vertex, String> value: values) {
            String src = value.f0.getPropertyValue("type").toString();
//            String src = value.f0.get

            if (sources.contains(src)){
                out.collect(value.f1);
                break;
            }
            sources.add(src);
        }
    }
}
