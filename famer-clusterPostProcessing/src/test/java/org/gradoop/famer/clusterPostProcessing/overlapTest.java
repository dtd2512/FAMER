package org.gradoop.famer.clusterPostProcessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.*;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import scala.math.Ordering;

import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

/**
 */
public class overlapTest {
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
             inputDir = args[0];//"/home/alieh/temp/ol5/";
//            String outputDir = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/Test-Result/20/Graphs/ClusteringResult/"+i+"/resolve-star1/";

            JSONDataSource dataSource =
                    new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
            LogicalGraph in = dataSource.getLogicalGraph();
//            String outputDir = "/home/alieh/git44Prject/sources/geographicalDataSet/clusteringResultClusters/g"+i+"/star1-OverlapRes/";





            in = in.callForGraph(new OverlapResolveStrengthNoMrg(0d));
        DataSet<Vertex> nonResolved = in.getVertices().flatMap(new FlatMapFunction<Vertex, Vertex>() {
            @Override
            public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
                if (vertex.getPropertyValue("ClusterId").toString().contains(","))
                    collector.collect(vertex);
            }
        });
       System.out.println("****** "+nonResolved.count());
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
