
package org.gradoop.famer.initialGraphGeneration.CSVLine2Vertex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;


/**
 * Used to convert a line of the DBLP data set to a Gradoop Vertex. The input string contains all data of an entity.
 */
public final class MusicRecordFields2Vertex3
        implements FlatMapFunction<Tuple10<String, String, String, String, String, String, String, String, String, String>, Vertex> {
    private VertexFactory vf;
    public MusicRecordFields2Vertex3 (VertexFactory VertexFactory){
        vf = VertexFactory;
    }
    private String removeUnknown(String input){
         input = input.toLowerCase().replaceAll("unknown","");
         input = input.toLowerCase().replaceAll("()","");
         input = input.toLowerCase().replaceAll("/[/]","");
        return input;
    }
    private String removeInitialDigits(String input){
        if(input.matches("[0-9.]*"))
            return "";
try {
    if(input.split("-").length >0 && input.split("-")[0].matches("[0-9.]*")){
        int index = input.indexOf("-");
        if (index!=-1)
            input = input.substring(index+1);
        else
            input = "";
    }

}
catch (Exception e){
    System.out.println(input+"LL "+input.split("-").length);
}
        return input;
    }

    public void flatMap(Tuple10<String, String,  String, String, String, String, String, String, String, String> in, Collector<Vertex> out) throws Exception {
        if (!in.f0.equals("TID")) {
            Vertex v = vf.createVertex();
//            v.setProperty("recId", in.f0);
            v.setProperty("clsId", in.f1);
            v.setProperty("type", in.f2);
//            v.setProperty("number", in.f3);
            v.setProperty("title", removeInitialDigits(in.f4));
//            v.setProperty("title", in.f4);

//            v.setProperty("length", in.f5);
            v.setProperty("artist", in.f6);
            v.setProperty("album", in.f7);
//            v.setProperty("year", in.f8);
//            v.setProperty("language", in.f9);
            out.collect(v);
        }
    }
}
