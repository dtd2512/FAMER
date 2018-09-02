package org.gradoop.famer.linking.selection.data_structures.Condition;

import java.io.Serializable;
import java.util.Collection;

/**
 */
public class ConditionList implements Serializable{
    private Collection<Condition> conditions;
    public ConditionList (Collection<Condition> Conditions){conditions = Conditions;}
    public Condition getCondition(String SimilarityFieldId){
        for (Condition condition:conditions)
            if (condition.getSimilarityFieldId().equals(SimilarityFieldId))
                return condition;
        return null;
    }
}
