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

/**
 */
public class test {
    public static void main (String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String outputDir = "/home/alieh/git44Prject/sources/geographicalDataSet/ovOPt5NoMerg.csv";

        FileWriter fw = new FileWriter(outputDir,true);
        fw.append("algo, graph,pre,rec,fm,ave,max,sing,all, per, perComp\n");
        fw.flush();
        for (int i = 5; i <= 8; i++) {
            String inputDir = "/home/alieh/git44Prject/sources/geographicalDataSet/clusteringResultClusters/g"+i+"/star2/";

            JSONDataSource dataSource =
                    new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
            LogicalGraph in = dataSource.getLogicalGraph();
//            ComputeSimGraphQualityMeasuresWithSameIds f = new ComputeSimGraphQualityMeasuresWithSameIds(in,"clusterId");
//            System.out.println("pre: "+f.computePrecision()+" rec: "+f.computeRecall()+" fm: "+f.computeFM());
//            multiEntity1SrcResolve me1srcRes = new multiEntity1SrcResolve(0d, 1, 5, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL);
//            in = in.callForGraph(me1srcRes);
            in = in.callForGraph(new overlapResolveOptimized5(true));



            ComputeClusteringQualityMeasures eval2 = new ComputeClusteringQualityMeasures("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",",",in, "",  false,true, false,4);
//            ComputeClusteringQualityMeasuresWithSameIds1Src eval2 =
//                    new ComputeClusteringQualityMeasuresWithSameIds1Src(in,"" , false);
//
            fw.append("Star2,"+"g"+i+","+eval2.computePrecision()+","+eval2.computeRecall()+","+eval2.computeFM()+","+
                    eval2.getAveClsterSize()+","+eval2.getMaxClsterSize()+","+eval2.getSingletons()+","+eval2.getClusterNo()+","+
                    eval2.getPerfectClusterNo()+","+eval2.getPerfectCompleteClusterNo()+"\n");
            fw.flush();



//            in = in.callForGraph(new multiEntity1SrcResolve(0d, 2, 5, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//            in = in.callForGraph(new multiEntity1SrcResolve(0d, 3, 5, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//
////            ComputeClusteringQualityMeasures eval3 = new ComputeClusteringQualityMeasures("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",",",in, "",  false,true, false);
//
//            ComputeClusteringQualityMeasuresWithSameIds1Src eval3 =
//                    new ComputeClusteringQualityMeasuresWithSameIds1Src(in,"" , false);
//            fw.append("phase2,"+"g"+i+","+eval3.computePrecision()+","+eval3.computeRecall()+","+eval3.computeFM()+","+
//                    eval3.getAveClsterSize()+","+eval3.getMaxClsterSize()+","+eval3.getSingletons()+","+eval3.getClusterNo()+","+
//                    eval3.getPerfectClusterNo()+","+eval3.getPerfectCompleteClusterNo()+"\n");
//            fw.flush();



        }
    }
}
