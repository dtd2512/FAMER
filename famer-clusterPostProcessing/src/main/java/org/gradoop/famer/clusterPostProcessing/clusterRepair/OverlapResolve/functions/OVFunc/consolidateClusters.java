package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.OVFunc;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class consolidateClusters implements GroupReduceFunction <Tuple2<Cluster, String>, Cluster>{
    @Override
    public void reduce(Iterable<Tuple2<Cluster, String>> input, Collector<Cluster> output) throws Exception {

        Collection<Cluster> clusters = new ArrayList<>();
        String clusterId = "";
        for (Tuple2<Cluster, String> cluster : input) {
            clusters.add(cluster.f0);
            clusterId = cluster.f1;
        }
        Cluster outCluster = null;
        if (clusterId.charAt(0) != 'M')
            outCluster = clusters.iterator().next();
//        if (clusters.size() > 1 && clusterId.charAt(0) != 'M') {
//            System.out.println(clusterId + " PPPPOOOOOOOJNJSHKHKSHJHSKJHKJSHKJHSKJHSJKHSKJHKJSHKJSHKJHSJKHSKJH");
//            for (Cluster t:clusters) {
//                for (Vertex v:t.getVertices())
//                    System.out.println(v.getPropertyValue("recId")+"      "+ v.getPropertyValue("ClusterId"));
//            }
//        }
        else {
            Collection<Vertex> vertices = new ArrayList<>();
            Collection<Edge> intraLinks = new ArrayList<>();
            Collection<Edge> interLinks = new ArrayList<>();
            for(Cluster cluster: clusters) {
                for (Vertex vertex : cluster.getVertices()) {
                    if (!vertices.contains(vertex))
                        vertices.add(vertex);
                }
                for (Edge edge : cluster.getIntraLinks()) {
                    if (!intraLinks.contains(edge))
                        intraLinks.add(edge);
                }
                for (Edge edge : cluster.getInterLinks()) {
                    if (!interLinks.contains(edge))
                        interLinks.add(edge);
                }
            }
            outCluster = new Cluster(vertices, intraLinks, interLinks, clusterId, "");

        }
        if (outCluster.getVertices().size() > 0)
            output.collect(outCluster);

    }
}
