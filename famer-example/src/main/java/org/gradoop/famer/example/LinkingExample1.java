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
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.AggregationComponentType;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.AggregatorComponent;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule;
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
 */
public class LinkingExample1 {
    // Linker Component
        // 1) Blocking Component
    private Boolean intraDataSetComparison = false;
    private BlockingMethod blockingMethod = BlockingMethod.STANDARD_BLOCKING;
            // 1-1) Blocking Key generator
    private KeyGenerationMethod keyGenerationMethod = KeyGenerationMethod.PREFIX_LENGTH;
    private String keyAttribute = "title";
    private Integer prefixLength = 1;
            //1-2) Parallelism Degree
    private Integer parallelismDegree = 4;


    // 2) Similarity Component
    private String similarityFieldId = "sim1";
    private SimilarityComputationMethod similarityComputationMethod = SimilarityComputationMethod.QGRAMS;
    private String sourceGraph = "dblp";
    private String sourceAttribute = "title";
    private String targetGraph = "acm";
    private String targetAttribute = "title";
    private Double weight = 1d;
    private Integer qgramLength = 3;
    private Boolean qgramPadding = false;
    private SimilarityComputationMethod qgramSecondMethod = SimilarityComputationMethod.DICE;

        // 3) Selection Component
            //3-1) Condition
    private String conditionId = "con1";
    private ConditionOperator conditionOperator = ConditionOperator.GREATER_EQUAL;
    private Double conditionThreshold = 0.8;
    private String relatedSimilarityFieldId = similarityFieldId;

    //3-2) Selection Rule := con1
    private SelectionComponentType ruleComponentType = SelectionComponentType.CONDITION_ID;
    private String ruleComponentValue = conditionId;
            //3-3) Similarity Aggregator Rule
    private AggregationComponentType aggregationComponentType = AggregationComponentType.SIMILARITY_FIELD_ID;
    private String aggregationComponentValue = similarityFieldId;
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

        GraphCollection output = new LinkingExample1().execute(input);

        output.writeTo(new JSONDataSink(outFolder + "/graphHeads.json", outFolder + "/vertices.json", outFolder + "/edges.json", grad_config));
        env.execute();
    }

    private GraphCollection execute(GraphCollection input) throws Exception {

        /************* step 1) Preparing input Configuration *******************/
        // Blocking
            // 1) keyGeneration Component
        PrefixLengthComponent keyGenerationComponent = new PrefixLengthComponent(keyGenerationMethod, keyAttribute, prefixLength);
        BlockingKeyGenerator blockingKeyGenerator = new BlockingKeyGenerator(keyGenerationComponent);

        StandardBlockingComponent blockingComponent = new StandardBlockingComponent(intraDataSetComparison, blockingMethod, blockingKeyGenerator, parallelismDegree);
        Collection<BlockingComponent> blockingComponents = new ArrayList<>();
        blockingComponents.add(blockingComponent);

        // Similarity Component
        org.gradoop.famer.linking.similarity_measuring.data_structures.QGramsComponent similarityComponent =
                new org.gradoop.famer.linking.similarity_measuring.data_structures.QGramsComponent(similarityFieldId, similarityComputationMethod, sourceGraph,
                sourceAttribute, targetGraph, targetAttribute, weight, qgramLength,qgramPadding, qgramSecondMethod);
        Collection<SimilarityComponent> similarityComponents = new ArrayList<>();
        similarityComponents.add(similarityComponent);

        //Selection Component
            //1) Condition
        Condition selectionCondition = new Condition(conditionId, relatedSimilarityFieldId, conditionOperator, conditionThreshold);
        Collection<Condition> selectionConditions = new ArrayList<>();
        selectionConditions.add(selectionCondition);
            //2) Selection Rule
        RuleComponent ruleComponent = new RuleComponent(ruleComponentType, ruleComponentValue);
        List<RuleComponent> ruleComponents = new ArrayList<>();
        ruleComponents.add(ruleComponent);
        SelectionRule selectionRule = new SelectionRule(ruleComponents);
            //3): Similarity Aggregator Rule
        AggregatorComponent aggregatorComponent = new AggregatorComponent(aggregationComponentType, aggregationComponentValue);
        List<AggregatorComponent> aggregatorComponents = new ArrayList<>();
        aggregatorComponents.add(aggregatorComponent);
        SimilarityAggregatorRule similarityAggregatorRule = new SimilarityAggregatorRule(aggregatorComponents);

        SelectionComponent selectionComponent = new SelectionComponent(selectionConditions, selectionRule, similarityAggregatorRule);



        LinkerComponent linkerComponent = new LinkerComponent(blockingComponents, similarityComponents, selectionComponent,
                keepCurrentEdges, recomputeSimilarityForCurrentEdges, edgeLabel);

        /************* step 2) Linking *******************/
         return input.callForCollection(new Linker(linkerComponent));
    }
}
