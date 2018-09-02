package org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule;


import java.io.Serializable;

/**
 */
public class AggregatorComponent implements Serializable{
    private AggregationComponentType componentType;
    private String value;
    public AggregatorComponent(AggregationComponentType ComponentType, String Value){componentType = ComponentType; value = Value;}
    public AggregationComponentType getComponentType(){return componentType;}
    public String getValue(){return value;}
}
