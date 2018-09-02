package org.gradoop.famer.linking.blocking.blocking_methods.data_structures;

import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;

import java.io.Serializable;

/**
 */
public class SortedNeighborhoodComponent extends BlockingComponent implements Serializable {
    private Integer windowSize;
    private BlockingKeyGenerator blockingKeyGenerator;
    public SortedNeighborhoodComponent(
    		Boolean IntraGraphComparison,
    		BlockingMethod BlockingMethod,
            BlockingKeyGenerator BlockingKeyGenerator,
            Integer WindowSize) {
        super(IntraGraphComparison, BlockingMethod);
        windowSize = WindowSize;
        blockingKeyGenerator = BlockingKeyGenerator;
    }
    public BlockingKeyGenerator getBlockingKeyGenerator(){return blockingKeyGenerator;}
    public Integer getWindowSize(){return windowSize;}
}
