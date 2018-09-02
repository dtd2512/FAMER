package org.gradoop.famer.phase.linking.selection;

import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.AggregationComponentType;
import org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.AggregatorComponent;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class SimilarityAggregatorRule {
    private List<AggregatorComponent> components;
    private Double aggregationThreshold;
    public SimilarityAggregatorRule(Element Content){
        components = new ArrayList<>();
        NodeList phaseList = Content.getElementsByTagName("Item");
        for (int i = 0; i < phaseList.getLength(); i++) {
            String itemContent = phaseList.item(i).getTextContent();
            switch (itemContent){
                case "(":
                    components.add(new AggregatorComponent(AggregationComponentType.OPEN_PARENTHESIS, itemContent));
                    break;
                case ")":
                    components.add(new AggregatorComponent(AggregationComponentType.CLOSE_PARENTHESIS, itemContent));
                    break;
                case "+":
                case "-":
                case "*":
                case "/":
                    components.add(new AggregatorComponent(AggregationComponentType.ALGEBRAIC_OPERATOR, itemContent));
                    break;
                default:
                    components.add(new AggregatorComponent(AggregationComponentType.SIMILARITY_FIELD_ID, itemContent));
                    break;
            }
        }
    }
    public org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule toSimilarityAggregatorRule(){
        return new org.gradoop.famer.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule(components, aggregationThreshold);
    }

}
