package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.functions2;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Iterator;

/**
 */

public class edge2ClusterPair2 implements GroupReduceFunction<Tuple5<Cluster, Integer, Double, Integer, String>, Tuple4<Cluster, Cluster, String, Double>> {
    @Override
    public void reduce(Iterable<Tuple5<Cluster, Integer, Double, Integer, String>> in, Collector<Tuple4<Cluster, Cluster, String, Double>> out) throws Exception {

        Cluster c1,c2;
        Iterator<Tuple5<Cluster, Integer, Double, Integer, String>> iterator = in.iterator();
        Tuple5<Cluster, Integer, Double, Integer, String> next = iterator.next();
        Integer degree = next.f1;
        Double simValue = next.f2;
        Integer strongness = next.f3;

        c1 = next.f0;


        Tuple5<Cluster, Integer, Double, Integer, String> nextnext = iterator.next();
        degree = Math.min(nextnext.f1, degree);
        c2 = nextnext.f0;

        Double coef = 1d/3d;
        strongness--;
        Double priorityValue = (0.3*simValue)+(0.2*(1/degree))+(0.5*(strongness));
//        Double priorityValue = (0.3*simValue)+(0.5*(strongness));
//        Double priorityValue = simValue;


        out.collect(Tuple4.of(c1, c2, c1.getComponentId(), priorityValue));

    }
}
