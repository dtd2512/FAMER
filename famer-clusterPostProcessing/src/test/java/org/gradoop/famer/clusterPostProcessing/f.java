package org.gradoop.famer.clusterPostProcessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.multiEntity1SrcResolve;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.OverlapResolveStrengthNoMrg;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.overlapResolveBulkIteration;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasures;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasuresWithSameIds;
import org.gradoop.famer.common.maxDeltaLinkSelection.maxLinkSrcSelection;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 */
public class f {
    public static void main (String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String inputDir = args[0];
        String outputDir = args[1];
        FileWriter fw = new FileWriter(outputDir, true);

        JSONDataSource dataSource =
                    new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);

        LogicalGraph in = dataSource.getLogicalGraph();
        for (int i=1;i<=2;i++) {
            Long l = in.callForGraph(new multiEntity1SrcResolve(0d, 1, 5, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL)).getVertices().count();
            System.out.print(l);
            fw.append(
            env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+"\n");
            fw.flush();
        }

    }
}
