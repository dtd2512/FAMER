package org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule;



import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityField;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityFieldList;

import java.io.Serializable;
import java.util.List;
import java.util.Stack;

/**
 */
public class SimilarityAggregatorRule implements Serializable{
    
	List<AggregatorComponent> components;
    Double aggregationThreshold;
    
    public SimilarityAggregatorRule(List<AggregatorComponent> aggregatorComponents, Double aggregationThreshold){
        components = aggregatorComponents;
        this.aggregationThreshold = aggregationThreshold;
    }

    public Double aggregateSimilarities(SimilarityFieldList similarityValues){

        Stack<AggregatorComponent> parsingStack = new Stack<>();
        Stack<AlgebraicOperator> algebraicOperators = new Stack<>();
        // added check for closing parenthesis for nested formulas
        if (!components.get(0).getComponentType().equals(AggregationComponentType.OPEN_PARENTHESIS)
        		|| !components.get(components.size() - 1).getComponentType().equals(AggregationComponentType.CLOSE_PARENTHESIS)){
            components.add(0, new AggregatorComponent(AggregationComponentType.OPEN_PARENTHESIS, "("));
            int size = components.size();
            components.add(size, new AggregatorComponent(AggregationComponentType.CLOSE_PARENTHESIS, ")"));
        }

        Double result = null;
        for (AggregatorComponent component:components){
            AggregationComponentType componentType = component.getComponentType();
            
            switch (componentType){
            
                case OPEN_PARENTHESIS:
                case SIMILARITY_FIELD_ID:
                case ALGEBRAIC_OPERATOR:
                // case COMPUTED_EXPRESSION added
                case COMPUTED_EXPRESSION:
                    parsingStack.push(component);
                    break;
                case CLOSE_PARENTHESIS: // write as a method
                    AggregatorComponent aggregatorComponent;
                    AggregationComponentType aggregatorComponentType;
                    
                    // processing operations in a pair of parentheses
                    do
                    {
                        aggregatorComponent = parsingStack.pop();
                        aggregatorComponentType = aggregatorComponent.getComponentType();
                        Double simDegree = -1d;
                        switch (aggregatorComponentType){
                            case SIMILARITY_FIELD_ID: 
                            	
                            	SimilarityField simField = similarityValues.getSimilarityField(aggregatorComponent.getValue());
                            	if(simField != null) {
                            		simDegree = simField.getSimilarityValue();
                            	}
                            	else {
                            		// replacing non-existent value with 0.0 for vertex pairs, that current similarity field is not related to
                            		simDegree = 0.0;
                            	}
                            	// no break on purpose due to SIMILARITY_FIELD_ID and COMPUTED_EXPRESSION both being operands
                            case COMPUTED_EXPRESSION:
                                if (aggregatorComponentType.equals(AggregationComponentType.COMPUTED_EXPRESSION)) {
                                    simDegree = Double.parseDouble(aggregatorComponent.getValue());
                                }
                                // storing the second operand, order changed due to stack operations
                                if (result == null) {
                                    result = simDegree;
                                }
                                // means that we have previously stored second operator and are now processing the first one
                                // ready to perform binary algebraic operation
                                else {
                                    AlgebraicOperator operator = algebraicOperators.pop();
                                    result = compute(operator, result, simDegree);
                                    AggregatorComponent newAggregatorComponent = new AggregatorComponent(AggregationComponentType.COMPUTED_EXPRESSION, result.toString());
                                    parsingStack.push(newAggregatorComponent);
                                    result = null;
                                }
                                break;
                            case ALGEBRAIC_OPERATOR:
                                algebraicOperators.push(AlgebraicOperator.valueOf(aggregatorComponent.getValue()));
                                break;
                        }
                    }
                    while (!aggregatorComponentType.equals(AggregationComponentType.OPEN_PARENTHESIS));
                    AggregatorComponent newAggregatorComponent = new AggregatorComponent(AggregationComponentType.COMPUTED_EXPRESSION, result.toString());
                    parsingStack.push(newAggregatorComponent);
                    result = null;
                    break;
            }
        }
        result = Double.parseDouble(parsingStack.pop().getValue());
        return result;
    }

    private Double compute (AlgebraicOperator Operator, Double Operand1, Double Operand2){
        Double result = -1d;
        if (Operator.equals(AlgebraicOperator.DIVISION)) {
            if (Operand2!=0)
                //result = Operand1 / Operand2;
            	// swapped due to order being changed by stack operations
            	result = Operand2 / Operand1;
        }
        else if (Operator.equals(AlgebraicOperator.MINUS))
            result =  Operand1 - Operand2;
        else if (Operator.equals(AlgebraicOperator.PLUS))
            result =  Operand1 + Operand2;
        else if (Operator.equals(AlgebraicOperator.MULTIPLY))
            result =  Operand1 * Operand2;
        return result;
    }
    
    public Double getAggregationThreshold() {
    	return aggregationThreshold;
    }
}
