package org.gradoop.famer.phase.linking.similarity_measuring;

import org.gradoop.famer.PhaseTitles;
import org.gradoop.famer.phase.Phase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class SimilarityMeasuringPhase extends Phase{
    Collection<SimilarityComponent> similarityComponents;
    public SimilarityMeasuringPhase(Element PhaseContent){
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        similarityComponents = new ArrayList<>();
        NodeList phaseList = PhaseContent.getElementsByTagName("SimilarityComponent");
        for (int i = 0; i < phaseList.getLength(); i++) {
            Element subPhaseContent = (Element) phaseList.item(i);
            similarityComponents.add(new SimilarityComponent(subPhaseContent));
        }
    }
    public Collection<SimilarityComponent> getSimilarityComponents(){return similarityComponents;}
}
