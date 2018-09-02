package org.gradoop.famer.clustering;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.*;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;

/**
 * Used to compute the quality of output of one org.gradoop.famer.clustering.clustering algorithms on one graph with different threshold
 * This class is not inherited from GradoopFlinkTestBase So, can be used for running on wdi cluster
 */

public class OnClusterTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
//        for (int i=Integer.parseInt(args[2]); i<=Integer.parseInt(args[3]); i++) {
            String inputDir = args[0]+args[2]+"/";
            JSONDataSource dataSource =
                    new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
        Integer algorithm = Integer.parseInt(args[3]);
//
//        FileWriter fw = new FileWriter(args[1],true);
//        fw.append("pre,rec,fm,ave,max,sing,all, per, perComp\n");
//        fw.flush();
            LogicalGraph input = dataSource.getLogicalGraph();
            LogicalGraph output = null;
        String outputPath = "";
        switch (algorithm) {
            case 1:
             outputPath = args[1] +
                    args[2] + "/ConCom/";

            output = input.callForGraph(new ConnectedComponents());
                break;
            case 2:
                 outputPath = args[1] +
                        args[2] + "/CCPiv/";

                output = input.callForGraph(new CorrelationClustering(false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 3:
                outputPath = args[1] +
                        args[2] + "/Center0/";

                output = input.callForGraph(new Center(0, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 4:
                outputPath = args[1] +
                        args[2] + "/Center1/";

                output = input.callForGraph(new Center(1, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 5:
                 outputPath = args[1] +
                        args[2] + "/MCenter0/";

                output = input.callForGraph(new MergeCenter(0, 0, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 6:
                outputPath = args[1] +
                        args[2] + "/MCenter1/";

                output = input.callForGraph(new MergeCenter(1, 0, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 7:
                outputPath = args[1] +
                        args[2] + "/Star10/";

                output = input.callForGraph(new Star(0, 1, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 8:
                outputPath = args[1] +
                        args[2] + "/Star11/";

                output = input.callForGraph(new Star(1, 1, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 9:
                outputPath = args[1] +
                        args[2] + "/Star20/";

                output = input.callForGraph(new Star(0, 2, false, clustering.ClusteringOutputType.GraphCollection));
                break;
            case 10:
                outputPath = args[1] +
                        args[2] + "/Star21/";

                output = input.callForGraph(new Star(1, 2, false, clustering.ClusteringOutputType.GraphCollection));
                break;
        }
//        ComputeClusteringQualityMeasuresWithSameIds1Src eval = new ComputeClusteringQualityMeasuresWithSameIds1Src(output, "",  false);
//        fw.append(eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+","+
//                eval.getAveClsterSize()+","+eval.getMaxClsterSize()+","+eval.getSingletons()+","+eval.getClusterNo()+","+
//                eval.getPerfectClusterNo()+","+eval.getPerfectCompleteClusterNo()+"\n");
            JSONDataSink jss = new JSONDataSink(outputPath + "graphHeads.json", outputPath + "vertices.json", outputPath + "edges.json", output.getConfig());
            jss.write(output);
            env.execute();
//        System.out.println("dddd "+ output.getEdges().count());

//        }

    }

}
