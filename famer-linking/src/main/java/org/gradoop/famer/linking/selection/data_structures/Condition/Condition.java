package org.gradoop.famer.linking.selection.data_structures.Condition;

import java.io.Serializable;

/**
 */
public class Condition implements Serializable{
    private String id;
    private String similarityFieldId;
    private ConditionOperator operator;
    private Double threshold;
    public Condition(String Id, String SimilarityFieldId, ConditionOperator Operator, Double Threshold){
        id = Id;
        similarityFieldId = SimilarityFieldId;
        operator = Operator;
        threshold = Threshold;
    }
    public String getSimilarityFieldId(){return similarityFieldId;}
    public String getId(){return id;}
    public Boolean checkCondition(Double SimilarityValue){
        switch (operator){
            case EQUAL:
                if (SimilarityValue.equals(threshold))
                    return true;
                break;
            case GREATER:
                if (SimilarityValue > threshold)
                    return true;
                break;
            case GREATER_EQUAL:
                if (SimilarityValue >= threshold)
                    return true;
                break;
            case NOT_EQUAL:
                if (!SimilarityValue.equals(threshold))
                    return true;
                break;
            case SMALLER:
                if (SimilarityValue < threshold)
                    return true;
                break;
            case SMALLER_EQUAL:
                if (SimilarityValue <= threshold)
                    return true;
                break;
        }
        return false;
    }
}
