package org.gradoop.famer.initialGraphGeneration.CSVLine2Vertex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

/**
 * Used to convert a line of the ACM data set to a Gradoop Vertex. The input string contains all data of an entity.
 */
public final class ACMLineToVertex
        implements FlatMapFunction<String , Vertex> {
    private VertexFactory vf;
    public ACMLineToVertex (VertexFactory VertexFactory){
        vf = VertexFactory;
    }

    public void flatMap(String in, Collector<Vertex> out) throws Exception {
        Vertex v = vf.createVertex();
        String[] newFields = new String[3];
        boolean firstLine = false;
        if (in.contains("id") && in.contains("author") && in.contains("title"))
            firstLine = true;
        if (!firstLine) {
            String[] splitedLine = in.split(",");
            newFields[0] = splitedLine[0];
            newFields[1] = splitedLine[1];
            int i;
            for(i=2;splitedLine.length>i && splitedLine[i].indexOf("\"") !=0 ;i++)
                newFields[1] += (", "+splitedLine[i]);

            newFields[2] = splitedLine[i];
            for(;splitedLine.length>i+1 && splitedLine[i+1].indexOf("\"") !=0 ;i++)
                newFields[2] +=(", "+splitedLine[i]);
//            Long vertexId = in.f0 +vertexIdStart+ 1;
//            v.setProperty("id",vertexId);
            v.setProperty("pubId",newFields[0].replaceAll("\"",""));
            v.setProperty("title",newFields[1].replaceAll("\"",""));
            v.setProperty("author", newFields[2].replaceAll("\"",""));
            //removed
//            v.setProperty("type", "acm");
            out.collect(v);
        }
    }
}
