package org.gradoop.famer.linking.selection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.selection.data_structures.Condition.Condition;
import org.gradoop.famer.linking.selection.data_structures.Condition.ConditionValue;
import org.gradoop.famer.linking.selection.data_structures.Condition.ConditionValueList;
import org.gradoop.famer.linking.selection.data_structures.SelectionComponent;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityField;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityFieldList;

import java.io.Serializable;
import java.util.Collection;

/**
 */
public class Selector implements FlatMapFunction<Tuple3<Vertex, Vertex, SimilarityFieldList>, Tuple3<Vertex, Vertex, Double>>, Serializable  {
    private SelectionComponent selectionComponent;

    public Selector(SelectionComponent SelectionComponent){
        selectionComponent = SelectionComponent;
    }
    @Override
    public void flatMap(Tuple3<Vertex, Vertex, SimilarityFieldList> input, Collector<Tuple3<Vertex, Vertex, Double>> output) throws Exception {
        ConditionValueList conditionValues = new ConditionValueList();
        for (Condition condition:selectionComponent.getConditions()){
        	
        	SimilarityField similarityField = input.f2.getSimilarityField(condition.getSimilarityFieldId());
        	// skip conditions, that do not exist for this pair of vertices
        	if(similarityField != null) {
                conditionValues.add(new ConditionValue(condition.getId(), condition.checkCondition(similarityField.getSimilarityValue())));
        	}

        }        
        
        // instead of using the library we calculate weighted average of similarities that are available for current pair of vertices
        // for arithmetic average equals weights are passed
        double similaritiesSum = 0.0;
        double weightSum = 0.0;
        for(SimilarityField similarityField: input.f2.getSimilarityFields()) {
        	similaritiesSum += ( similarityField.getSimilarityValue() * similarityField.getWeight() );
        	weightSum += similarityField.getWeight();
        }
        double aggSimilarites = similaritiesSum / weightSum;
        
        if(selectionComponent.getSelectionRuleEnabled()) {
            if(selectionComponent.getSelectionRule().check(conditionValues)) {
    	    	//Double aggregatedSimilarities = selectionComponent.getSimilarityAggregatorRule().aggregateSimilarities(input.f2);
            	if(selectionComponent.getAggregationRuleEnabled()) {
        	    	if(aggSimilarites >= selectionComponent.getSimilarityAggregatorRule().getAggregationThreshold()) {
        	    		output.collect(Tuple3.of(input.f0, input.f1, aggSimilarites));
        	    	}
            	}
            	else {
            		output.collect(Tuple3.of(input.f0, input.f1, aggSimilarites));
            	}

    	    } 
        }
        else {
	    	//Double aggregatedSimilarities = selectionComponent.getSimilarityAggregatorRule().aggregateSimilarities(input.f2);
        	if(selectionComponent.getAggregationRuleEnabled()) {
    	    	if(aggSimilarites >= selectionComponent.getSimilarityAggregatorRule().getAggregationThreshold()) {
    	    		output.collect(Tuple3.of(input.f0, input.f1, aggSimilarites));
    	    	}
        	}
        	else {
        		output.collect(Tuple3.of(input.f0, input.f1, aggSimilarites));
        	}
        }
    }
}
