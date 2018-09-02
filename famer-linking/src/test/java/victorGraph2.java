import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 */
public class victorGraph2 {
    public static void main (String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String edgePath = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM/edges.csv";
        String graphPath = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM0.8/vertices/";
        DataSet<Tuple3<String, String, Double>> edges = env.readCsvFile(edgePath).types(String.class, String.class, Double.class);
        EdgeFactory ef = config.getEdgeFactory();
        JSONDataSource jsd = new JSONDataSource(graphPath+"graphHeads.json",graphPath+"vertices.json",graphPath+"edges.json",config);
        LogicalGraph lg = jsd.getLogicalGraph();


        DataSet<Tuple2<GradoopId, String>> vertexId_clsId = lg.getVertices().map(new vertex2vertex_gradoopId_clsId());
        DataSet<Tuple3<GradoopId, GradoopId, Double>> realEdges = edges.join(vertexId_clsId).where(0).equalTo(1).with(new JoinFunction<Tuple3<String,String,Double>, Tuple2<GradoopId,String>, Tuple3<GradoopId,String,Double>>() {
            @Override
            public Tuple3<GradoopId, String, Double> join(Tuple3<String, String, Double> in1, Tuple2<GradoopId, String> in2) throws Exception {
                return Tuple3.of(in2.f0, in1.f1, in1.f2);
            }
        }).join(vertexId_clsId).where(1).equalTo(1).with(new JoinFunction<Tuple3<GradoopId,String,Double>, Tuple2<GradoopId,String>, Tuple3<GradoopId,GradoopId,Double>>() {
            @Override
            public Tuple3<GradoopId, GradoopId, Double> join(Tuple3<GradoopId, String, Double> in1, Tuple2<GradoopId, String> in2) throws Exception {
                return Tuple3.of(in1.f0, in2.f0, in1.f2);
            }
        });



        DataSet<Edge> graphEdgs =  realEdges.map(new createEdge(ef));
        lg = lg.getConfig().getLogicalGraphFactory().fromDataSets(lg.getVertices(), graphEdgs);

        String outputPath = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM/LogicalGraph/";
        JSONDataSink jds = new JSONDataSink(outputPath+"graphHeads.json",outputPath+"vertices.json",outputPath+"edges.json",config);
        jds.write(lg);
        env.setParallelism(1);
        env.execute();
    }

    private static class createEdge implements MapFunction<Tuple3<GradoopId, GradoopId, Double>, Edge> {
        private EdgeFactory ef;
        public createEdge(EdgeFactory EdgeFactory) {
            ef = EdgeFactory;
        }

        @Override
        public Edge map(Tuple3<GradoopId, GradoopId, Double> input) throws Exception {
            Edge edge = ef.createEdge(input.f0, input.f1);
            edge.setProperty("value", input.f2);
            return edge;
        }
    }

    private static class vertex2vertex_gradoopId_clsId implements MapFunction<Vertex, Tuple2<GradoopId, String>> {
        @Override
        public Tuple2<GradoopId, String> map(Vertex vertex) throws Exception {
            return Tuple2.of(vertex.getId(), vertex.getPropertyValue("dsId").toString());
        }
    }
}
