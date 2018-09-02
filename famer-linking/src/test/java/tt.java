import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 */
public class tt {
    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        DataSet<Tuple1<String>> edges = null;
        for (int i=1;i<=10;i++) {
            String path = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM/training_data_withResult/" + i + ".csv";
//            String newPath = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/edges.csv";
            DataSet<Tuple10<String, String, Boolean, Double, Double, Double, Double, Double, Double, Double>> lines = env.readCsvFile(path).fieldDelimiter(";")
                    .types(String.class, String.class, Boolean.class, Double.class, Double.class
                    , Double.class, Double.class, Double.class, Double.class, Double.class);
            DataSet<Tuple1<String>> newEdges = lines.flatMap(new FlatMapFunction<Tuple10<String, String, Boolean, Double, Double, Double, Double, Double, Double, Double>, Tuple1<String>>() {
                @Override
                public void flatMap(Tuple10<String, String, Boolean, Double, Double, Double, Double, Double, Double, Double> in, Collector<Tuple1<String>> collector) throws Exception {
                    if (in.f2){
                        Double ave = (in.f3+in.f4+in.f5+in.f6+in.f7+in.f8+in.f9)/7;
                        collector.collect(Tuple1.of(in.f0+","+in.f1+","+ave));
                    }
                }
            });
            if (edges == null)
                edges = newEdges;
            else
                edges = edges.union(newEdges);


        }
        edges.writeAsCsv("/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM/edges.csv");
        env.setParallelism(1);
        env.execute();

    }
}
