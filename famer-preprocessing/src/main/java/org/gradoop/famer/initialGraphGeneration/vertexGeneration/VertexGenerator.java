package org.gradoop.famer.initialGraphGeneration.vertexGeneration;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.initialGraphGeneration.CSVLine2Vertex.*;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 */
public class VertexGenerator {
    private DSTitle dsTitle;
    private String dsPath;
    public VertexGenerator (String DSPath, DSTitle DSTitle){dsPath = DSPath;  dsTitle = DSTitle;}
    public DataSet<Vertex> generateVertex(GradoopFlinkConfig config) {
        DataSet<Vertex> output = null;
        DataSet<String> lines = config.getExecutionEnvironment().readTextFile(dsPath);
        switch (dsTitle){
            case ACM:
                output = lines.flatMap(new ACMLineToVertex(config.getVertexFactory()));
                break;
            case DBLP:
                output = lines.flatMap(new DBLPLineToVertex(config.getVertexFactory()));
                break;
            case GOOGLE_SCHOLAR:
                output = lines.flatMap(new GSLineToVertex(config.getVertexFactory()));
                break;
//            case "Music":
//                output = lines.flatMap(new MusicRecordFields2Vertex(config.getVertexFactory()));
//                break;
            case GENERATOR_CORRUPTOR:
                output = lines.flatMap(new GeCoVertexGenerator(config.getVertexFactory()));
                break;

        }

        return output;
    }
}
