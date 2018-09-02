package org.gradoop.famer.initialGraphGeneration.CSVLine2Vertex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;


/**
 * Used to convert a line of the DBLP data set to a Gradoop Vertex. The input string contains all data of an entity.
 */
public final class MusicRecordFields2Vertex
        implements FlatMapFunction<Tuple9<String, String, String, String, String, String, String, String, String>, Vertex> {
    private VertexFactory vf;
    public MusicRecordFields2Vertex (VertexFactory VertexFactory){
        vf = VertexFactory;
    }

    public void flatMap(Tuple9<String,  String, String, String, String, String, String, String, String> in, Collector<Vertex> out) throws Exception {
        if (!in.f0.equals("TID")) {
            Vertex v = vf.createVertex();
            v.setProperty("recId", in.f0);
            v.setProperty("type", in.f1);
            v.setProperty("number", in.f2);
            v.setProperty("title", in.f3);
            v.setProperty("length", in.f4);
            v.setProperty("artist", in.f5);
            v.setProperty("album", in.f6);
            v.setProperty("year", in.f7);
            v.setProperty("language", in.f8);
            out.collect(v);
        }
    }
}
