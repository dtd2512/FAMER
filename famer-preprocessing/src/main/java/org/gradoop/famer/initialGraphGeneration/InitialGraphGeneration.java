package org.gradoop.famer.initialGraphGeneration;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.initialGraphGeneration.vertexGeneration.DSTitle;
import org.gradoop.famer.initialGraphGeneration.vertexGeneration.VertexGenerator;
import org.gradoop.famer.initialGraphGeneration.vertexGeneration.assignGraphId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 */
public class InitialGraphGeneration {
    private String inputPath;
    private DSTitle dsName;
    private String label;
    private GradoopFlinkConfig config;
    public InitialGraphGeneration (String InputPath, DSTitle DSName, String Label,
                                   GradoopFlinkConfig Config) {
        inputPath = InputPath;
        dsName = DSName;
        label = Label;
        config=Config;
    }
    public LogicalGraph generateGraph()  {
        VertexGenerator vertexGenerator = new VertexGenerator(inputPath, dsName);

        GraphHead graphHead = config.getGraphHeadFactory().createGraphHead(label);
        DataSet<GraphHead> graphHeads = config.getExecutionEnvironment().fromElements(graphHead);

        DataSet<Vertex> vertices = vertexGenerator.generateVertex(config);
        vertices = vertices.map(new assignGraphId(graphHead.getId()));

        DataSet<Edge> edges = config.getLogicalGraphFactory().createEmptyGraph().getEdges();

        return config.getLogicalGraphFactory().fromDataSets(graphHeads, vertices, edges);
    }
}
