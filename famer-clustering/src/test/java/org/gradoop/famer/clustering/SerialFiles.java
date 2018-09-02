package org.gradoop.famer.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Test;

/**
 * Used to generate input csv files from json graphs for serial org.gradoop.famer.clustering.clustering
 */
public class SerialFiles  {

    public static  void main(String[] args) throws Exception {
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment());
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputDir = args[0]; // example: "/home/alieh/Documents/Datasets/DBLPGS";
        JSONDataSource dataSource =
                new JSONDataSource (inputDir + "/Json/graphHeads.json", inputDir + "/Json/vertices.json", inputDir + "/Json/edges.json", config);
        LogicalGraph initialInput = dataSource.getLogicalGraph();
//        DataSet<Edge> filteredEdges = initialInput.getEdges().filter(new FilterEdges(0.80));
//        DataSet<Tuple2<Edge, Double>> filteredEdgesWithDegrees = filteredEdges.map(new MapFunction<Edge, Tuple2<Edge, Double>>() {
//            public  Tuple2<Edge, Double> map(Edge in) {
//                return Tuple2.of(in,Double.parseDouble(in.getPropertyValue("value").toString()));
//            }
//        });
//        DataSet<Edge> finalfilteredEdges = filteredEdgesWithDegrees.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<Edge, Double>, Edge>() {
//            public void reduce(Iterable<Tuple2<Edge, Double>> ins, Collector<Edge> out) {
//                Edge e = null;
//                for (Tuple2<Edge, Double> i : ins) {
//                    e = i.f0;
//                }
//                if (e != null)
//                    out.collect(e);
//            }
//        });
//
//        LogicalGraph input = LogicalGraph.fromDataSets(initialInput.getGraphHead(), initialInput.getVertices(), finalfilteredEdges, config);



        DataSet<Tuple2<GradoopId, Long>> vertices = initialInput.getVertices().map(new MapFunction<Vertex, Tuple2<GradoopId, Long>>() {
            public Tuple2<GradoopId, Long> map(Vertex in) {
                return Tuple2.of(in.getId(),Long.parseLong(in.getPropertyValue("VertexPriority").toString()));
            }
        });
        DataSet<Tuple3<GradoopId, GradoopId, Double>> edges = initialInput.getEdges().map(new MapFunction<Edge, Tuple3<GradoopId, GradoopId, Double>>() {
            public Tuple3<GradoopId, GradoopId, Double> map(Edge in) {
                return Tuple3.of(in.getSourceId(),in.getTargetId(), Double.parseDouble(in.getPropertyValue("value").toString()));
            }
        });
        vertices.writeAsCsv(inputDir+"/test/vertices80.csv","\n", ",");
        edges.writeAsCsv(inputDir+"/test/edges80.csv","\n", ",");
        String outputDir= inputDir+"/Json/";
        initialInput.writeTo(new JSONDataSink(
                outputDir + "serialgraphHeads80.json",
                outputDir + "serialvertices80.json",
                outputDir + "serialedges80.json",
                config));
        env.setParallelism(1);
        env.execute();

    }
}
