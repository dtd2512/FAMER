package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")

public class link2link_srcInfo_trgtInfo implements MapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple3<Edge, String, String>>{
    private String infoTitle;
    public link2link_srcInfo_trgtInfo(String InfoTitle){ infoTitle = InfoTitle;}
    @Override
    public Tuple3<Edge, String, String> map(Tuple3<Edge, Vertex, Vertex> value) throws Exception {
        return Tuple3.of(value.f0, value.f1.getPropertyValue(infoTitle).toString(), value.f2.getPropertyValue(infoTitle).toString());
    }
}
