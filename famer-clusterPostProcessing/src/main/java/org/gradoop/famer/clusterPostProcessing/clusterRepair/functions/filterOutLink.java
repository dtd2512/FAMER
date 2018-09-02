package org.gradoop.famer.clusterPostProcessing.clusterRepair.functions;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 */
public class filterOutLink implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Edge> {
    private Collection<String> attributes;
    public filterOutLink (Collection<String> Attributes) { attributes = Attributes;}
    public filterOutLink (){}

    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Edge> out) throws Exception {

        Boolean isOut = true;
////
        for (String attribute:attributes){
            if (Boolean.parseBoolean(in.f1.getPropertyValue(attribute).toString()) || Boolean.parseBoolean(in.f2.getPropertyValue(attribute).toString()))
                isOut = false;
        }
        if (isOut)
            out.collect(in.f0);
//        isCompletePerfect
//
//
//
//        Boolean srcStatus = Boolean.parseBoolean(in.f1.getPropertyValue("isCompletePerfect").toString());
//        Boolean trgtStatus = Boolean.parseBoolean(in.f2.getPropertyValue("isCompletePerfect").toString());
//        if (!(srcStatus || trgtStatus)){
//            out.collect(in.f0);
//        }
    }
}
