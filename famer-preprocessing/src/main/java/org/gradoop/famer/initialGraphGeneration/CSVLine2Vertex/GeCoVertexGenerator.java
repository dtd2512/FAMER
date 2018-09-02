package org.gradoop.famer.initialGraphGeneration.CSVLine2Vertex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

/**
 * Used to convert a line of the data set anf an integer to a Gradoop Vertex. The input string contains all data of an entity and
 * the integer is the generated id of the file. This id indicates the source id. Since,
 * this dataset is from multiple data sources.
 */
public final class GeCoVertexGenerator implements FlatMapFunction<String , Vertex> {
    private VertexFactory vf;
    public GeCoVertexGenerator(VertexFactory VertexFactory){vf=VertexFactory;}

    public void flatMap(String value, Collector<Vertex> out) throws Exception {
        String s="";
        try {
            s = value;
//            System.out.println(value.f0);

            String[] values = value.split(",");
            Vertex v = vf.createVertex();
            v.setProperty("recId", values[0] );//new
            v.setProperty("clsId", values[0]);//old


            //type
            // v.setProperty("type", "geco" + value.f1);
            v.setProperty("name", values[1]);
            v.setProperty("surname", values[2]);
            if (values.length > 3 && values[3] != null)
                v.setProperty("suburb", values[3]);
            else
                v.setProperty("suburb", "");

            if (values.length > 4 && values[4] != null)
                v.setProperty("postcod", values[4]);
            else
                v.setProperty("postcod", "");

            out.collect(v);
        }
        catch (Exception e){
            System.out.println(s);
        }

    }
}
