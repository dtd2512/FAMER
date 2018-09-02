package org.gradoop.famer.clusterPostProcessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.ParallelResolveMsgPassing3;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.*;
import org.gradoop.famer.clustering.clustering;
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
public class yyy {
    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);



        String inputDir = "/home/alieh/temp/newTests/CLIP/1/";

        JSONDataSource dataSource =
                new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
        LogicalGraph in = dataSource.getLogicalGraph();
        in =  in.callForGraph(new ParallelResolveMsgPassing3(0, clustering.ClusteringOutputType.GraphCollection));
        in.getVertices().print();





//
//        DataSet<Vertex> vertices = in.getVertices().flatMap(new FlatMapFunction<Vertex, Vertex>() {
//            @Override
//            public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
//                if (vertex.getPropertyValue("ClusterId").toString().contains(","))
//                    collector.collect(vertex);
//            }
//        });

//        System.out.println(in.getVertices().count());

    }

}
