package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.OVFunc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Comparator;

/**
 */
public class ClusterSorter implements Comparator<Tuple2<Cluster, Double>> {
    @Override
    public int compare(Tuple2<Cluster, Double> cs1, Tuple2<Cluster, Double> cs2) {
        return cs1.f1.compareTo(cs2.f1) * -1;
    }

    @Override
    public Comparator<Tuple2<Cluster, Double>> reversed() {
        return null;
    }
}