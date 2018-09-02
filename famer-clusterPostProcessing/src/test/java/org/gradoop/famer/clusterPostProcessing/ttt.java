package org.gradoop.famer.clusterPostProcessing;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasuresWithSameIds;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;

/**
 */
public class ttt {
    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String inputDir = args[0];
        String outputDir = args[1];

        FileWriter fw = new FileWriter(outputDir, true);
        fw.append("pre, rec, fm\n");
        fw.flush();
        JSONDataSource dataSource =
                new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
        LogicalGraph in = dataSource.getLogicalGraph();
        ComputeSimGraphQualityMeasuresWithSameIds e = new ComputeSimGraphQualityMeasuresWithSameIds(in,"");
        fw.append(e.computePrecision()+","+e.computeRecall()+","+e.computeFM()+"\n");
        fw.flush();

    }
}
