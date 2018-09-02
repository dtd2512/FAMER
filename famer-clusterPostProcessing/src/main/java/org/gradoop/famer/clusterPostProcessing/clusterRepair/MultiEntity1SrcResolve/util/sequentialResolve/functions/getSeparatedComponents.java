package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class getSeparatedComponents implements FlatMapFunction <Cluster, Cluster>{
    @Override
    public void flatMap(Cluster value, Collector<Cluster> out) throws Exception {
        if (value.getInterLinks().size() == 0)
            out.collect(value);
    }
}
