package org.gradoop.famer.linking.blocking.blocking_methods.data_structures;

import java.io.Serializable;

/**
 */
public class BlockingComponent implements Serializable{
    private Boolean intraGraphComparison;
    private BlockingMethod blockingMethod;
    public BlockingComponent (Boolean IntraGraphComparison, BlockingMethod BlockingMethod){
        intraGraphComparison = IntraGraphComparison;
        blockingMethod = BlockingMethod;
    }
    public BlockingMethod getBlockingMethod(){return blockingMethod;}
    public Boolean getIntraGraphComparison(){return intraGraphComparison;}
}
