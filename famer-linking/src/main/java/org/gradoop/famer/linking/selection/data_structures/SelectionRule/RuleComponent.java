package org.gradoop.famer.linking.selection.data_structures.SelectionRule;

import java.io.Serializable;

/**
 */
public class RuleComponent implements Serializable{
    private SelectionComponentType componentType;
    private String value;
    public RuleComponent(SelectionComponentType ComponentType, String Value){componentType = ComponentType; value = Value;}
    public SelectionComponentType getComponentType(){return componentType;}
    public String getValue(){return value;}
}
