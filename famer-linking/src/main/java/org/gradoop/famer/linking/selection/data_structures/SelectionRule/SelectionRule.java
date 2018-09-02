package org.gradoop.famer.linking.selection.data_structures.SelectionRule;

import org.gradoop.famer.linking.selection.data_structures.Condition.ConditionValue;
import org.gradoop.famer.linking.selection.data_structures.Condition.ConditionValueList;

import java.io.Serializable;
import java.util.List;
import java.util.Stack;

/**
 */
public class SelectionRule implements Serializable{
    private List<RuleComponent> components;
    public SelectionRule(List<RuleComponent> RuleComponents){
        components = RuleComponents;
    }


    public Boolean check(ConditionValueList conditionValues){

        Stack<RuleComponent> parsingStack = new Stack<>();
        Stack<SelectionOperator> selectionOperators = new Stack<>();
        if (!components.get(0).getComponentType().equals(SelectionComponentType.OPEN_PARENTHESIS)
        		|| !components.get( components.size() - 1 ).getComponentType().equals(SelectionComponentType.CLOSE_PARENTHESIS)){
            components.add(0, new RuleComponent(SelectionComponentType.OPEN_PARENTHESIS, "("));
            int size = components.size();
            components.add(size, new RuleComponent(SelectionComponentType.CLOSE_PARENTHESIS, ")"));
        }

        Boolean result = null;
        for (RuleComponent component:components){
            SelectionComponentType componentType = component.getComponentType();
            switch (componentType){
                case OPEN_PARENTHESIS:
                case CONDITION_ID:
                case SELECTION_OPERATOR:
                case COMPUTED_EXPRESSION:
                    parsingStack.push(component);
                    break;
                case CLOSE_PARENTHESIS: // write as a method
                    RuleComponent ruleComponent;
                    SelectionComponentType ruleComponentType;
                    do{
                        ruleComponent = parsingStack.pop();
                        ruleComponentType = ruleComponent.getComponentType();
                        Boolean conditionValue = null;
                        switch (ruleComponentType){
                            case CONDITION_ID:
                            	ConditionValue cValue = conditionValues.getConditionValue(ruleComponent.getValue());
                            	if(cValue != null) {
                            		conditionValue = cValue.getValue();
                            	}
                            	else {
                            		// case where condition is absent for this pair of vertices
                            		conditionValue = false;
                            	}                            	
                            case COMPUTED_EXPRESSION:
                                if (ruleComponentType.equals(SelectionComponentType.COMPUTED_EXPRESSION)) {
                                	conditionValue = Boolean.parseBoolean(ruleComponent.getValue());
                                }
                                
                                if (result == null) {
                                    result = conditionValue;
                                }
                                else {
                                    SelectionOperator operator = selectionOperators.pop();
                                    result = compute(operator, result, conditionValue);
                                    RuleComponent newRuleComponent = new RuleComponent(SelectionComponentType.COMPUTED_EXPRESSION, result.toString());
                                    parsingStack.push(newRuleComponent);
                                    result = null;
                                }
                                break;
                            case SELECTION_OPERATOR:
                                selectionOperators.push(SelectionOperator.valueOf(ruleComponent.getValue()));
                                break;
                        }

                    }while (!ruleComponentType.equals(SelectionComponentType.OPEN_PARENTHESIS));
                    RuleComponent newRuleComponent = new RuleComponent(SelectionComponentType.COMPUTED_EXPRESSION, result.toString());
                    parsingStack.push(newRuleComponent);
                    result = null;
                    break;
            }
        }
        result = Boolean.parseBoolean(parsingStack.pop().getValue());
        return result;
    }

    private Boolean compute (SelectionOperator Operator, Boolean Operand1, Boolean Operand2){
        Boolean result = null;
        if (Operator.equals(SelectionOperator.OR))
            result =  Operand1 | Operand2;
        else if (Operator.equals(SelectionOperator.AND))
            result =  Operand1 & Operand2;
        return result;
    }
}
