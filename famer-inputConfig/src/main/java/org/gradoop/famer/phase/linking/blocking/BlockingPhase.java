package org.gradoop.famer.phase.linking.blocking;

import org.gradoop.famer.PhaseTitles;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingMethod;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.SortedNeighborhoodComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.StandardBlockingComponent;
import org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.StandardBlocking;
import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.KeyGenerationComponent;
import org.gradoop.famer.phase.Phase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 */
public class BlockingPhase extends Phase {
    private Boolean intraGraphComparison;
    private BlockingMethod blockingMethod;
    private BlockingKeyGenerationPhase blockingKeyGenerationPhase;
    private Integer integerParam;

    public BlockingPhase(Element PhaseContent) {
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        intraGraphComparison = Boolean.parseBoolean(PhaseContent.getElementsByTagName("IntraGraphComparison").item(0).getTextContent());
        blockingMethod = BlockingMethod.valueOf(PhaseContent.getElementsByTagName("BlockingMethod").item(0).getTextContent());
        switch (blockingMethod){
            case CARTESIAN_PRODUCT:
                break;
            case STANDARD_BLOCKING:
            case SORTED_NEIGHBORHOOD:
                integerParam = Integer.parseInt(PhaseContent.getElementsByTagName("integerParam").item(0).getTextContent());
                blockingKeyGenerationPhase = new BlockingKeyGenerationPhase((Element)PhaseContent.getElementsByTagName("Phase").item(0));
                break;
        }
    }
    public BlockingComponent toBlockingComponent(){
        BlockingKeyGenerator blockingKeyGenerator = new BlockingKeyGenerator(blockingKeyGenerationPhase.toKeyGenerationComponent());
        switch (blockingMethod){
            case CARTESIAN_PRODUCT:
                return new BlockingComponent(intraGraphComparison, blockingMethod);
            case STANDARD_BLOCKING:
                return new StandardBlockingComponent(intraGraphComparison, blockingMethod,
                        blockingKeyGenerator, integerParam);
            case SORTED_NEIGHBORHOOD:
                return  new SortedNeighborhoodComponent(intraGraphComparison, blockingMethod,
                        blockingKeyGenerator, integerParam);

        }
        return null;
    }
}



