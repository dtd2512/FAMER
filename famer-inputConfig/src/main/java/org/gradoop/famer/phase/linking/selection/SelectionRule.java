package org.gradoop.famer.phase.linking.selection;

import org.gradoop.famer.linking.selection.data_structures.SelectionRule.RuleComponent;
import org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionComponentType;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class SelectionRule {
    List<RuleComponent> components;
    public SelectionRule (Element Content){
        components = new ArrayList<>();
        NodeList phaseList = Content.getElementsByTagName("Item");
        for (int i = 0; i < phaseList.getLength(); i++) {
            String itemContent = phaseList.item(i).getTextContent();
            switch (itemContent){
                case "(":
                    components.add(new RuleComponent(SelectionComponentType.OPEN_PARENTHESIS, itemContent));
                    break;
                case ")":
                    components.add(new RuleComponent(SelectionComponentType.CLOSE_PARENTHESIS, itemContent));
                    break;
                case "&":
                    components.add(new RuleComponent(SelectionComponentType.SELECTION_OPERATOR, itemContent));
                    break;
                case "|":
                    components.add(new RuleComponent(SelectionComponentType.SELECTION_OPERATOR, itemContent));
                    break;
                default:
                    components.add(new RuleComponent(SelectionComponentType.CONDITION_ID, itemContent));
                    break;
            }

        }
    }
    public org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule toSelectionRule (){
        return new org.gradoop.famer.linking.selection.data_structures.SelectionRule.SelectionRule(components);
    }

}
