package org.gradoop.famer.phase.linking;

import org.gradoop.famer.PhaseTitles;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.gradoop.famer.linking.linking.data_structures.LinkerComponent;
import org.gradoop.famer.phase.Phase;
import org.gradoop.famer.phase.linking.blocking.BlockingPhase;
import org.gradoop.famer.phase.linking.selection.SelectionPhase;
import org.gradoop.famer.phase.linking.similarity_measuring.SimilarityComponent;
import org.gradoop.famer.phase.linking.similarity_measuring.SimilarityMeasuringPhase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class LinkingPhase extends Phase {
    private Collection<BlockingPhase> blockingPhases;
    private SimilarityMeasuringPhase similarityMeasuringPhase;
    private SelectionPhase selectionPhase;
    private Boolean keepCurrentEdges;
    private Boolean recomputeSimilarityForCurrentEdges;
    private String edgeLabel;
    public LinkingPhase(Element PhaseContent) {
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        blockingPhases = new ArrayList<>();
        NodeList phaseList = PhaseContent.getElementsByTagName("Phase");
        for (int i = 0; i < phaseList.getLength(); i++) {
            Element subPhaseContent = (Element) phaseList.item(i);
            linkingPhaseContent(subPhaseContent);
        }
        keepCurrentEdges = Boolean.parseBoolean(PhaseContent.getElementsByTagName("KeepCurrentEdges").item(0).getTextContent());
        recomputeSimilarityForCurrentEdges = Boolean.parseBoolean(PhaseContent.getElementsByTagName("RecomputeSimilarityForCurrentEdges").item(0).getTextContent());
        edgeLabel = PhaseContent.getElementsByTagName("EdgeLabel").item(0).getTextContent();
    }
    private void linkingPhaseContent(Element PhaseContent) {
        PhaseTitles phaseTitle = PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent());
        switch (phaseTitle) {
            case BLOCKING:
                BlockingPhase blockingPhase = new BlockingPhase(PhaseContent);
                blockingPhases.add(blockingPhase);
                break;
            case SIMILARITY_MEASURING:
                similarityMeasuringPhase = new SimilarityMeasuringPhase(PhaseContent);
                break;
            case SELECTION:
                selectionPhase = new SelectionPhase(PhaseContent);
                break;
        }
    }
    public LinkerComponent toLinkerComponet(){
        Collection<BlockingComponent> blockingComponents = new ArrayList<>();
        Collection<org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent> similarityComponents = new ArrayList<>();
        for (BlockingPhase blockingPhase : blockingPhases)
            blockingComponents.add(blockingPhase.toBlockingComponent());
        for (SimilarityComponent similarityComponent : similarityMeasuringPhase.getSimilarityComponents())
            similarityComponents.add(similarityComponent.toSimilarityComponent());
        return new LinkerComponent(blockingComponents, similarityComponents, selectionPhase.toSelectionComponent(), keepCurrentEdges, recomputeSimilarityForCurrentEdges, edgeLabel);
    }
}


































