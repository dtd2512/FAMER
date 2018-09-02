import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.famer.linking.selection.data_structures.Condition.Condition;
import org.gradoop.famer.linking.selection.data_structures.Condition.ConditionOperator;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.RuleComponent;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionComponentType;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** */
public class t {
    public static void main(String args[]) throws Exception {
//         String conditionId = "con1";
//         ConditionOperator conditionOperator = ConditionOperator.GREATER_EQUAL;
//         Double conditionThreshold = 0.8;
//        //3-2) Selection Rule := con1
//         SelectionComponentType ruleComponentType = SelectionComponentType.CONDITION_ID;
//         String ruleComponentValue = conditionId;
//
//
//        Condition selectionCondition = new Condition(conditionId, "", conditionOperator, conditionThreshold);
//        Collection<Condition> selectionConditions = new ArrayList<>();
//        selectionConditions.add(selectionCondition);
//        //2) Selection Rule
//        RuleComponent ruleComponent = new RuleComponent(ruleComponentType, ruleComponentValue);
//        List<RuleComponent> ruleComponents = new ArrayList<>();
//        ruleComponents.add(ruleComponent);
//        SelectionRule selectionRule = new SelectionRule(ruleComponents);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        DataSet<Tuple4<String, String, String, String>> lines = env.readCsvFile("/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/RawInputFiles/musicbrainz-20-A01.csv").types(String.class, String.class, String.class, String.class);
        DataSet<Vertex> vertices = lines.map(new createVertex(config.getVertexFactory()));
        LogicalGraph lg = config.getLogicalGraphFactory().fromDataSets(vertices);
        String outputPath = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/FM0.8/vertices/";
        JSONDataSink jds = new JSONDataSink(outputPath+"graphHeads.json",outputPath+"vertices.json",outputPath+"edges.json",config);
        jds.write(lg);
        env.setParallelism(1);
        env.execute();


    }

    private static class createVertex implements MapFunction<Tuple4<String, String, String, String>, Vertex> {
        private VertexFactory vf;
        public createVertex(VertexFactory vertexFactory) {
            vf = vertexFactory;
        }

        @Override
        public Vertex map(Tuple4<String, String, String, String> input) throws Exception {
            Vertex vertex = vf.createVertex();
            vertex.setProperty("dsId", input.f0);
            vertex.setProperty("clusterId", input.f1);
            vertex.setProperty("type", input.f3);

            return vertex;
        }
    }
}
