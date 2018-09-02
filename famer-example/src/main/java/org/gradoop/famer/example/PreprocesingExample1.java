package org.gradoop.famer.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.initialGraphGeneration.InitialGraphGeneration;
import org.gradoop.famer.initialGraphGeneration.vertexGeneration.DSTitle;
import org.gradoop.famer.logicalGraphList2GraphCollection.toGraphCollection;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class PreprocesingExample1 {
    // DBLP
    private static String DBLPDataSetPath = "sources/DBLP-ACM/RawInputFiles/dblp.csv";
    private String DBLPGraphLabel = "dblp";
    private DSTitle DBLPDSTitle = DSTitle.DBLP;
    // ACM
    private static String ACMDataSetPath = "sources/DBLP-ACM/RawInputFiles/acm.csv";
    private String ACMGraphLabel = "acm";
    private DSTitle ACMDSTitle = DSTitle.ACM;



    public static void main(String args[]) throws Exception {
        String outFolder = args[0];
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig(env);

        DBLPDataSetPath = new File(DBLPDataSetPath).getAbsolutePath().toString();
        ACMDataSetPath = new File(ACMDataSetPath).getAbsolutePath().toString();

        GraphCollection output = new PreprocesingExample1().execute(grad_config);

        output.writeTo(new JSONDataSink(outFolder + "/graphHeads.json", outFolder + "/vertices.json", outFolder + "/edges.json", grad_config));
        env.execute();
    }
    public GraphCollection execute(GradoopFlinkConfig GradoopFlinkConfig) throws Exception {
        LogicalGraph DBLPGraph = new InitialGraphGeneration(DBLPDataSetPath, DBLPDSTitle, DBLPGraphLabel, GradoopFlinkConfig).generateGraph();
        LogicalGraph ACMGraph = new InitialGraphGeneration(ACMDataSetPath, ACMDSTitle, ACMGraphLabel, GradoopFlinkConfig).generateGraph();
        Collection<LogicalGraph> logicalGraphs = new ArrayList<>();
        logicalGraphs.add(DBLPGraph);
        logicalGraphs.add(ACMGraph);
        return new toGraphCollection().execute(logicalGraphs);
    }
}
