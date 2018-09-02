package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 */
public class filterOutVertexByPropertyValue implements FlatMapFunction <Vertex, Vertex>{
//    private String property;
//    private String value;
//    public filterOutVertexByPropertyValue (String Property, String Value){property = Property; value = Value;}
//    @Override
//    public void flatMap(Vertex vertex, Collector<Vertex> out) throws Exception {
//        if (vertex.getPropertyValue(property).toString().equals(value))
//            out.collect(vertex);
//    }
    private Collection<String> attributes;

    public filterOutVertexByPropertyValue (Collection<String> Attributes){ attributes = Attributes;}
    @Override
    public void flatMap(Vertex vertex, Collector<Vertex> out) throws Exception {
        Boolean isOut = false;
        for (String attribute: attributes) {
            if (Boolean.parseBoolean(vertex.getPropertyValue(attribute).toString()))
                isOut = true;
        }
        if (isOut)
            out.collect(vertex);
    }
}
