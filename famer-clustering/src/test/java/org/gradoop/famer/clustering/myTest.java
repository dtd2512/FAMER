package org.gradoop.famer.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.*;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import scala.Int;

import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

/**
 * Used to compute the quality of output of one org.gradoop.famer.clustering.clustering algorithms on one graph with different threshold
 * This class is not inherited from GradoopFlinkTestBase So, can be used for running on wdi cluster
 */

public class myTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);


        String inputDir = args[0];
        String outputDir = args[1];
        Integer algo = Integer.parseInt(args[2]);
        FileWriter fw = new FileWriter(outputDir, true);
        fw.append("algo, runtime\n");
        fw.flush();
//        for (int i = 1; i <= 5; i++) {
//            String outputDir = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM0.98/clusteringClusters/" + i + "/";
//            Integer algo = i;

            JSONDataSource dataSource =
                    new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
            LogicalGraph in = dataSource.getLogicalGraph();
            switch (algo) {
                case 1:
                    in = in.callForGraph(new CorrelationClustering(false, clustering.ClusteringOutputType.GraphCollection));
                    break;
                case 2:
                    in = in.callForGraph(new Center(0, false, clustering.ClusteringOutputType.GraphCollection));
                    break;
                case 3:
                    in = in.callForGraph(new MergeCenter(0, 0d, false, clustering.ClusteringOutputType.GraphCollection));
                    break;
                case 4:
                    in = in.callForGraph(new Star(0, 1, false, clustering.ClusteringOutputType.GraphCollection));
                    break;
                case 5:
                    in = in.callForGraph(new Star(0, 2, false, clustering.ClusteringOutputType.GraphCollection));
                    break;
            }
        System.out.print(in.getEdges().count());
        fw.append(algo+","+env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+"\n");
        fw.flush();
//            in = in.callForGraph(new CorrelationClustering(false, clustering.ClusteringOutputType.GraphCollection));
//
//
//            JSONDataSink jsd = new JSONDataSink(outputDir + "graphHeads.json", outputDir + "vertices.json", outputDir + "edges.json", config);
//            jsd.write(in);
//            env.setParallelism(1);
//            env.execute();
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);




    }


    }


