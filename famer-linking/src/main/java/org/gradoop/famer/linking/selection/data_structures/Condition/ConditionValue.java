package org.gradoop.famer.linking.selection.data_structures.Condition;

import java.io.Serializable;

/**
 */
public class ConditionValue implements Serializable{
    private String id;
    private Boolean value;
    public ConditionValue (String Id, Boolean Value){id= Id; value = Value;}
    public void setValue(Boolean Value){value = Value;}
    public Boolean getValue(){return value;}
    public String getId(){return id;}
}
