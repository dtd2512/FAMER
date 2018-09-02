package org.gradoop.famer.linking.selection.data_structures.Condition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class ConditionValueList implements Serializable{
    private Collection<ConditionValue> conditionValues;
    public ConditionValueList (Collection<ConditionValue> ConditionValues){conditionValues = ConditionValues;}
    public ConditionValueList (){conditionValues = new ArrayList<>();}

    public ConditionValue getConditionValue(String Id){
        for (ConditionValue conditionValue:conditionValues)
            if (conditionValue.getId().equals(Id))
                return conditionValue;
        return null;
    }
    public void add (ConditionValue ConditionValue){
        conditionValues.add(ConditionValue);
    }
}
