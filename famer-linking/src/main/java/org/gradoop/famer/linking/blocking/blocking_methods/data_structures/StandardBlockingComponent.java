package org.gradoop.famer.linking.blocking.blocking_methods.data_structures;

import org.gradoop.famer.linking.blocking.key_generation.BlockingKeyGenerator;

import java.io.Serializable;

/**
 */
public class StandardBlockingComponent extends BlockingComponent implements Serializable {
    private BlockingKeyGenerator blockingKeyGenerator;
    private Integer parallelismDegree;
    public StandardBlockingComponent(
    		Boolean IntraGraphComparison, 
    		BlockingMethod BlockingMethod,
            BlockingKeyGenerator BlockingKeyGenerator, 
            Integer ParallelismDegree) {
        super(IntraGraphComparison, BlockingMethod);
        blockingKeyGenerator = BlockingKeyGenerator;
        parallelismDegree = ParallelismDegree;
    }
    public BlockingKeyGenerator getBlockingKeyGenerator(){return blockingKeyGenerator;}
    public Integer getParallelismDegree(){return parallelismDegree;}
}
