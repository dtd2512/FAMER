package org.gradoop.famer.linking.selection.data_structures;

import org.gradoop.famer.linking.selection.data_structures.Condition.Condition;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule;

import java.io.Serializable;
import java.util.Collection;

/**
 */
public class SelectionComponent implements Serializable{
	
    private Collection<Condition> conditions;
    private SelectionRule selectionRule;
    private SimilarityAggregatorRule similarityAggregatorRule;
    private Boolean aggregationRuleEnabled;
    private Boolean selectionRuleEnabled;
    
    public SelectionComponent(
    		Collection<Condition> conditions, 
    		SelectionRule selectionRule,
            SimilarityAggregatorRule similarityAggregatorRule,
            Boolean aggregationRuleEnabled,
            Boolean selectionRuleEnabled){
        this.conditions = conditions;
        this.selectionRule = selectionRule;
        this.similarityAggregatorRule = similarityAggregatorRule;
        this.aggregationRuleEnabled = aggregationRuleEnabled;
        this.selectionRuleEnabled = selectionRuleEnabled; 
    }
    
    public Collection<Condition> getConditions(){return conditions;}
    public SelectionRule getSelectionRule(){return selectionRule;}
    public SimilarityAggregatorRule getSimilarityAggregatorRule(){return similarityAggregatorRule;}
    public Boolean getAggregationRuleEnabled() {return aggregationRuleEnabled;}
    public Boolean getSelectionRuleEnabled() {return selectionRuleEnabled;}
}

