package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;


import org.apache.flink.api.common.functions.Partitioner;

/**
 */

public class PartitionVertices implements Partitioner<Integer> {
    @Override
    public int partition(Integer key, int numPartitions)
    { return  key;}
}