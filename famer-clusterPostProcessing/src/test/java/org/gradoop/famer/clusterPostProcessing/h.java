package org.gradoop.famer.clusterPostProcessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.multiEntity1SrcResolve;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.OverlapResolveStrengthNoMrg;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.overlapResolveBulkIteration;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.overlapResolveOptimized5;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasures;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasuresWithSameIds;
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
public class h {
    public static void main (String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String outputDir = args[1];
        String outputDir2 = args[3];
        String inputDir = args[0];

        FileWriter fw = new FileWriter(outputDir,true);
        fw.append("algo, graph,pre,rec,fm,ave,max,sing,all, per, perComp\n");
        fw.flush();
        FileWriter fw2 = new FileWriter(outputDir2,true);
        fw2.append("algo, graph,runtime\n");
        fw2.flush();
//        for (int i =1 ; i <= 3; i++) {

            JSONDataSource dataSource =
                    new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
for (int algo=1;algo<=3;algo++) {
    LogicalGraph in = dataSource.getLogicalGraph();
//
    switch (algo) {
        case 1:
            in = in.callForGraph(new OverlapResolveStrengthNoMrg(0d));
            break;
        case 2:
            in = in.callForGraph(new overlapResolveOptimized5(true));
            break;
        case 3:
            in = in.callForGraph(new overlapResolveOptimized5(false));
    }



//            ComputeClusteringQualityMeasures eval2 = new ComputeClusteringQualityMeasures
//                    ("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",",",in, "",  false,true, false,4);
    ComputeClusteringQualityMeasuresWithSameIds1Src eval2 = new ComputeClusteringQualityMeasuresWithSameIds1Src
            (in, "", false);

//
    fw.append(algo + "," + "g" + args[2] + "," + eval2.computePrecision() + "," + eval2.computeRecall() + "," + eval2.computeFM() + "," +
            eval2.getAveClsterSize() + "," + eval2.getMaxClsterSize() + "," + eval2.getSingletons() + "," + eval2.getClusterNo() + "," +
            eval2.getPerfectClusterNo() + "," + eval2.getPerfectCompleteClusterNo() + "\n");
    fw.flush();

    fw2.append(algo+","+ args[2]+","+env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+"\n");
    fw2.flush();



        }
    }
}
