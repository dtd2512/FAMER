package org.gradoop.famer.initialGraphGeneration.CSVLine2Vertex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

/**
 * Used to convert a line of the Scholar data set to a Gradoop Vertex. The input string contains all data of an entity.
 */
public final class GSLineToVertex
        implements FlatMapFunction<String , Vertex> {
    private VertexFactory vf;
    public GSLineToVertex (VertexFactory VertexFactory){
        vf = VertexFactory;
    }
    public void flatMap(String in, Collector<Vertex> out) throws Exception {
        Vertex v = vf.createVertex();
        String[] newFields = new String[5];
        int prevDoubleQutationIndex = 0;
        int firstDoubleQutationIndex = 0;
        int pairs = 0;
        boolean firstLine = false;
        if (in.indexOf("\"") < 0)
            firstLine = true;
        if (!firstLine) {
            while (pairs <= 3) {
                firstDoubleQutationIndex = in.indexOf("\"", prevDoubleQutationIndex);
                if (firstDoubleQutationIndex < 0) {
                    newFields[pairs] = "\"\"";
                    break;
                }
                int secondDoubleQutationIndex = in.indexOf("\"", firstDoubleQutationIndex + 1);
                try {
                    newFields[pairs] = (in.substring(firstDoubleQutationIndex, secondDoubleQutationIndex) + "\"");
                }
                catch (Exception ex) {
                    System.out.println(firstDoubleQutationIndex + "   " + secondDoubleQutationIndex);
                    System.out.println(in);
                }
                pairs++;
                prevDoubleQutationIndex = secondDoubleQutationIndex + 1;
            }
            String[] remaining = in.substring(prevDoubleQutationIndex).split(",");
            if (firstDoubleQutationIndex > 0)
                newFields[4] = remaining[1];
            else
                newFields[4] = remaining[2];
//            Long vertexId = in.f0+vertexIdStart+1;
//            v.setProperty("id",vertexId);
            v.setProperty("pubId", newFields[0].replaceAll("\"",""));
            v.setProperty("title", newFields[1].replaceAll("\"",""));
            v.setProperty("author", newFields[2].replaceAll("\"",""));
            v.setProperty("venue", newFields[3].replaceAll("\"",""));
            v.setProperty("year", newFields[4].replaceAll("\"",""));
            //type
//            v.setProperty("type", "gs");
            out.collect(v);
        }
    }
}
