package org.gradoop.famer.clustering.GenerateConnectedComponentGraph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import java.io.File;

/**
 * Used to generate connected component graph from dblp-acm or dblp-gs existent graphs.
 * */

public class DblpAcmDblpGs extends GradoopFlinkTestBase {

    @Test
    public void runTest() throws Exception {
        ExecutionEnvironment env = config.getExecutionEnvironment();

        String inputDir = "../sources/DBLP-ACM/";
        String graphSrcFolder = inputDir + "Test-Result/test2/Graphs/";
        File folder = new File(graphSrcFolder);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            if (file.isDirectory()) {
                String folderName = graphSrcFolder + file.getName();
                JSONDataSource dataSource =
                        new JSONDataSource(folderName + "/graphHeads.json", folderName + "/vertices.json", folderName + "/edges.json", config);
                LogicalGraph input = dataSource.getLogicalGraph();
//                input = input.callForGraph(new org.gradoop.famer.clustering.GenerateConnectedComponentGraph());
            }
        }
        DataSet<Tuple2<String, String>> srcPubIdTrgetPubId = env.readCsvFile(inputDir+"RawInputFiles/pm.csv").types(String.class, String.class);
        DataSet<Tuple2<String,String>> distinctPubIds = srcPubIdTrgetPubId.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String,String>>() {
            public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String,String>> out) throws Exception {
                out.collect(Tuple2.of(in.f0,"dblp"));
                out.collect(Tuple2.of(in.f1,"acm"));
            }
        }).distinct(0);
        DataSet<Vertex> vertices = distinctPubIds.map(new pubId2Vertex(config.getVertexFactory()));
        DataSet<Tuple2<Vertex, String>> vertexPubId = vertices.map(new MapFunction<Vertex, Tuple2<Vertex, String>>() {
            public Tuple2<Vertex, String> map(Vertex in) throws Exception {
                return Tuple2.of(in, in.getPropertyValue("pubId").toString());
            }
        });
        srcPubIdTrgetPubId = srcPubIdTrgetPubId.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            public Tuple2<String, String> map(Tuple2<String, String> in) throws Exception {
                return Tuple2.of(in.f0.replaceAll("\"",""), in.f1.replaceAll("\"",""));
            }
        });
        DataSet <Tuple2<GradoopId, GradoopId>> srcIdTrgetId = srcPubIdTrgetPubId.join(vertexPubId).where(0).equalTo(1).with(new JoinFunction<Tuple2<String,String>, Tuple2<Vertex,String>, Tuple2<GradoopId,String>>() {
            public  Tuple2<GradoopId,String> join (Tuple2<String,String> in1, Tuple2<Vertex,String> in2){
                return Tuple2.of(in2.f0.getId(), in1.f1);
            }
        }).join(vertexPubId).where(1).equalTo(1).with(new JoinFunction<Tuple2<GradoopId, String>, Tuple2<Vertex, String>, Tuple2<GradoopId, GradoopId>>() {
            public Tuple2<GradoopId, GradoopId> join(Tuple2<GradoopId, String> in1, Tuple2<Vertex, String> in2) throws Exception {
                return Tuple2.of(in1.f0, in2.f0.getId());
            }
        });
        DataSet<Edge> edges = srcIdTrgetId.map(new srcIdTrgetId2Edge (config.getEdgeFactory()));
        LogicalGraph input = config.getLogicalGraphFactory().fromDataSets(vertices,edges);
        String outputDir= inputDir+"PerfectMapGraph/";
        input.writeTo(new JSONDataSink(
                outputDir + "graphHeads.json",
                outputDir + "vertices.json",
                outputDir + "edges.json",
                config));
        env.setParallelism(1);
        env.execute();
    }








    public final class pubId2Vertex
            implements MapFunction<Tuple2<String, String> , Vertex> {
        private VertexFactory vf;
        public pubId2Vertex (VertexFactory VertexFactory){
            vf = VertexFactory;
        }

        public Vertex map(Tuple2<String, String> in) throws Exception {
            Vertex v = vf.createVertex();
            v.setProperty("pubId",in.f0.replaceAll("\"",""));
            v.setProperty("type",in.f1);
            return v;
        }
    }
    public final class srcIdTrgetId2Edge
            implements MapFunction<Tuple2<GradoopId, GradoopId> , Edge> {
        private EdgeFactory ef;
        public srcIdTrgetId2Edge (EdgeFactory VertexFactory){
            ef = VertexFactory;
        }
        public Edge map(Tuple2<GradoopId, GradoopId> in) throws Exception {
            Edge e = ef.createEdge(in.f0, in.f1);
            e.setProperty("value",1);
            return e;
        }
    }


}
