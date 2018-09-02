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
public class PreprocesingExample2 {
    private static String DataSetFolder = "sources/GeneratorCorruptor/05parties/5-ocp20/";
    private DSTitle Example2DSTitle = DSTitle.GENERATOR_CORRUPTOR;

    // Src0
    private static String DataSetPath0 = DataSetFolder + "ncvr_numrec_5000_modrec_2_ocp_20_myp_0_nump_5.csv";
    private String GraphLabel0 = "src0";
    // Src1
    private static String DataSetPath1 = DataSetFolder + "ncvr_numrec_5000_modrec_2_ocp_20_myp_1_nump_5.csv";
    private String GraphLabel1 = "src1";
    // Src2
    private static String DataSetPath2 = DataSetFolder + "ncvr_numrec_5000_modrec_2_ocp_20_myp_2_nump_5.csv";
    private String GraphLabel2 = "src2";
    // Src3
    private static String DataSetPath3 = DataSetFolder + "ncvr_numrec_5000_modrec_2_ocp_20_myp_3_nump_5.csv";
    private String GraphLabel3 = "src3";
    // Src4
    private static String DataSetPath4 = DataSetFolder + "ncvr_numrec_5000_modrec_2_ocp_20_myp_4_nump_5.csv";
    private String GraphLabel4 = "src4";


    public static void main(String args[]) throws Exception {
        String outFolder = args[0];
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig(env);

        DataSetPath0 = new File(DataSetPath0).getAbsolutePath().toString();
        DataSetPath1 = new File(DataSetPath1).getAbsolutePath().toString();
        DataSetPath2 = new File(DataSetPath2).getAbsolutePath().toString();
        DataSetPath3 = new File(DataSetPath3).getAbsolutePath().toString();
        DataSetPath4 = new File(DataSetPath4).getAbsolutePath().toString();


        GraphCollection output = new PreprocesingExample1().execute(grad_config);

        output.writeTo(new JSONDataSink(outFolder + "/graphHeads.json", outFolder + "/vertices.json", outFolder + "/edges.json", grad_config));
        env.execute();
    }
    public GraphCollection execute(GradoopFlinkConfig GradoopFlinkConfig) throws Exception {
        LogicalGraph Graph0 = new InitialGraphGeneration(DataSetPath0, Example2DSTitle, GraphLabel0, GradoopFlinkConfig).generateGraph();
        LogicalGraph Graph1 = new InitialGraphGeneration(DataSetPath1, Example2DSTitle, GraphLabel1, GradoopFlinkConfig).generateGraph();
        LogicalGraph Graph2 = new InitialGraphGeneration(DataSetPath2, Example2DSTitle, GraphLabel2, GradoopFlinkConfig).generateGraph();
        LogicalGraph Graph3 = new InitialGraphGeneration(DataSetPath3, Example2DSTitle, GraphLabel3, GradoopFlinkConfig).generateGraph();
        LogicalGraph Graph4 = new InitialGraphGeneration(DataSetPath4, Example2DSTitle, GraphLabel4, GradoopFlinkConfig).generateGraph();

        Collection<LogicalGraph> logicalGraphs = new ArrayList<>();
        logicalGraphs.add(Graph0);logicalGraphs.add(Graph1);logicalGraphs.add(Graph2);
        logicalGraphs.add(Graph3);logicalGraphs.add(Graph4);
        return new toGraphCollection().execute(logicalGraphs);
    }
}
