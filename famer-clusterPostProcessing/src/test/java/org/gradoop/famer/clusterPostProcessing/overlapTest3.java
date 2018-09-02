package org.gradoop.famer.clusterPostProcessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.*;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

/**
 */
public class overlapTest3 {
    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);


//        FileWriter fw = new FileWriter("/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/OverlapPaper.csv",true);
//        fw.append("algo, graph,pre,rec,fm,ave,max,sing,all, per, perComp\n");
////        String algo = "star"+args[2];
////        fw.write("algo, runtime\n");
//        fw.flush();
        String inputDir = "";

        ;

//        for (int i = 1; i <= 3; i++) {
        inputDir = "/home/alieh/git44Prject/sources/GeneratorCorruptor-PeterChristen/clusteringResultGraphs/5parties/2/star2/";
//            String outputDir = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/Test-Result/20/Graphs/ClusteringResult/"+i+"/resolve-star1/";

        JSONDataSource dataSource =
                new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
        LogicalGraph in = dataSource.getLogicalGraph();
//            String outputDir = "/home/alieh/git44Prject/sources/geographicalDataSet/clusteringResultClusters/g"+i+"/star1-OverlapRes/";






        DataSet<Tuple1<Integer>> nonResolved = in.getVertices().flatMap(new FlatMapFunction<Vertex, Tuple1<Integer>>() {
            @Override
            public void flatMap(Vertex vertex, Collector<Tuple1<Integer>> collector) throws Exception {

                    collector.collect(Tuple1.of(vertex.getPropertyValue("ClusterId").toString().split(",").length));
            }
        }).maxBy(0);
        for (Tuple1<Integer> i :nonResolved.collect())
            System.out.println("****** "+i.f0);
//            in = in.callForGraph(new RemoveInterClustersLinks());
////
////
//            JSONDataSink sink = new JSONDataSink(outputDir+"graphHeads.json",outputDir+"vertices.json", outputDir+"edges.json", config);
//            sink.write(in);
//            env.setParallelism(1);
//            env.execute();
//////            ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",",", in, "",  false,true,false,4);
//            ComputeClusteringQualityMeasuresWithSameIds1Src eval = new ComputeClusteringQualityMeasuresWithSameIds1Src
//                    (in,"",false);
////
//            fw.append("star1,"+""+i+","+eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+","+
//                eval.getAveClsterSize()+","+eval.getMaxClsterSize()+","+eval.getSingletons()+","+eval.getClusterNo()+","+
//                eval.getPerfectClusterNo()+","+eval.getPerfectCompleteClusterNo()+"\n");
//            fw.flush();

//        }
    }

}
