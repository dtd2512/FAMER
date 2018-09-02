package org.gradoop.famer.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingMethod;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.StandardBlockingComponent;
import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.KeyGenerationMethod;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.PrefixLengthComponent;
import org.gradoop.famer.linking.linking.Linker;
import org.gradoop.famer.linking.linking.data_structures.LinkerComponent;
import org.gradoop.famer.linking.selection.data_structures.Condition.Condition;
import org.gradoop.famer.linking.selection.data_structures.Condition.ConditionOperator;
import org.gradoop.famer.linking.selection.data_structures.SelectionComponent;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.RuleComponent;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionComponentType;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionOperator;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.AggregationComponentType;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.AggregatorComponent;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule;
import org.gradoop.famer.linking.similarity_measuring.data_structures.JaroWinklerComponent;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComputationMethod;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class LinkingExample2 {
    // Linker Component
    // 1) Blocking Component
    private Boolean intraDataSetComparison = false;
    private BlockingMethod blockingMethod = BlockingMethod.STANDARD_BLOCKING;
    // 1-1) Blocking Key generator
    private KeyGenerationMethod keyGenerationMethod = KeyGenerationMethod.PREFIX_LENGTH;
    private String keyAttribute = "surname";
    private Integer prefixLength = 1;
    //1-2) Parallelism Degree
    private Integer parallelismDegree = 4;


    // 2) Similarity Component
    private String similarityFieldId0 = "sim0";
    private SimilarityComputationMethod similarityComputationMethod0 = SimilarityComputationMethod.JAROWINKLER;
    private String sourceGraph0 = "src0";
    private String sourceAttribute0 = "name";
    private String targetGraph0 = "src1";
    private String targetAttribute0 = "name";
    private Double weight0 = 1d;
    private Double simThreshold0 = 0.8;

    private String similarityFieldId1 = "sim1";
    private SimilarityComputationMethod similarityComputationMethod1 = SimilarityComputationMethod.JAROWINKLER;
    private String sourceGraph1 = "src0";
    private String sourceAttribute1 = "surname";
    private String targetGraph1 = "src1";
    private String targetAttribute1 = "surname";
    private Double weight1 = 1d;
    private Double simThreshold1 = 0.8;

    /// how many components??????


    // 3) Selection Component
    //3-1) Condition
    private String conditionId0 = "con0";
    private ConditionOperator conditionOperator0 = ConditionOperator.GREATER_EQUAL;
    private Double conditionThreshold0 = 0.8;
    private String relatedSimilarityFieldId0 = similarityFieldId0;


    private String conditionId1 = "con1";
    private ConditionOperator conditionOperator1 = ConditionOperator.GREATER_EQUAL;
    private Double conditionThreshold1 = 0.8;
    private String relatedSimilarityFieldId1 = similarityFieldId1;

    /// how many components??????


    //3-2) Selection Rule := con1 & con 2
    private SelectionComponentType ruleComponentType0 = SelectionComponentType.CONDITION_ID;
    private String ruleComponentValue0 = conditionId0;

    private SelectionComponentType ruleComponentType1 = SelectionComponentType.SELECTION_OPERATOR;
    private String ruleComponentValue1 = SelectionOperator.AND.toString();

    private SelectionComponentType ruleComponentType2 = SelectionComponentType.CONDITION_ID;
    private String ruleComponentValue2 = conditionId1;

    //3-3) Similarity Aggregator Rule := (sim0+sim1)/2
    private AggregationComponentType aggregationComponentType0 = AggregationComponentType.OPEN_PARENTHESIS;
    private String aggregationComponentValue0 = "(";

    private AggregationComponentType aggregationComponentType1 = AggregationComponentType.SIMILARITY_FIELD_ID;
    private String aggregationComponentValue1 = similarityFieldId0;

    private AggregationComponentType aggregationComponentType2 = AggregationComponentType.ALGEBRAIC_OPERATOR;
    private String aggregationComponentValue2 = "+";

    private AggregationComponentType aggregationComponentType3 = AggregationComponentType.SIMILARITY_FIELD_ID;
    private String aggregationComponentValue3 = similarityFieldId1;

    private AggregationComponentType aggregationComponentType4 = AggregationComponentType.CLOSE_PARENTHESIS;
    private String aggregationComponentValue4 = ")";

    private AggregationComponentType aggregationComponentType5 = AggregationComponentType.ALGEBRAIC_OPERATOR;
    private String aggregationComponentValue5 = "/";

    private AggregationComponentType aggregationComponentType6 = AggregationComponentType.COMPUTED_EXPRESSION;
    private String aggregationComponentValue6 = "2";
    // 4)
    private Boolean keepCurrentEdges = false;
    // 5)
    private Boolean recomputeSimilarityForCurrentEdges = false;
    // 6)
    private String edgeLabel = "value";

    public static void main(String args[]) throws Exception {
        String srcFolder = args[0];
        String outFolder = args[1];
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig(env);
        JSONDataSource dataSource = new JSONDataSource(srcFolder + "graphHeads.json", srcFolder + "vertices.json", srcFolder + "edges.json", grad_config);
        GraphCollection input = dataSource.getGraphCollection();

        GraphCollection output = new LinkingExample2().execute(input);
        output.writeTo(new JSONDataSink(outFolder + "/graphHeads.json", outFolder + "/vertices.json", outFolder + "/edges.json", grad_config));
        env.execute();
    }

    private GraphCollection execute(GraphCollection input) throws Exception {
        return null;
    }
}
