package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.*;


/**
 *
 */
public class resolveReducer2 implements GroupReduceFunction <Tuple3<String, Vertex, Cluster>, Vertex> {
    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, Cluster>> in, Collector<Vertex> out) throws Exception {

        List<Tuple2<Cluster, Double>> cluster_associationDegree = new ArrayList<>();
        Vertex vertex = null;
        for (Tuple3<String, Vertex, Cluster> elem : in){
            cluster_associationDegree.add(Tuple2.of(elem.f2, elem.f2.getAssociationDegree4(elem.f0)));
            vertex = elem.f1;
        }
        Collections.sort(cluster_associationDegree, new Sorter());
        Collection<String> mergedClusterIdList = isMergable(cluster_associationDegree, vertex);
        if (mergedClusterIdList.size()==0)
            vertex.setProperty("ClusterId", cluster_associationDegree.get(0).f0.getClusterId());
        else {
            String mergedClusterId="M";
            for (String id:mergedClusterIdList)
                mergedClusterId+=id;
            vertex.setProperty("ClusterId", mergedClusterId);
            //simple imp: merge iff the clusters has no other overlap
            String merged = "M";
            for (Tuple2<Cluster, Double> elem :cluster_associationDegree){
                if (mergedClusterIdList.contains(elem.f0.getClusterId())){
                    merged += elem.f0.getClusterId();
                }
            }
            vertex.setProperty("ClusterId", merged);
            for (Tuple2<Cluster, Double> elem :cluster_associationDegree){
                if (mergedClusterIdList.contains(elem.f0.getClusterId())){
                    for (Vertex v:elem.f0.getVertices()){
                        if(!v.getId().toString().equals(vertex.getId().toString())) {
                            v.setProperty("ClusterId", merged);
                            out.collect(v);
                        }
                    }
                }
            }
//            for (Tuple2<Cluster,Double> elem : cluster_associationDegree){
//                for (Vertex v : elem.f0.getVertices()){
//                    if(!v.getId().toString().equals(vertex.getId().toString())) {
//                        String[] clusterIds = v.getPropertyValue("ClusterId").toString().split(",");
//                        String finalClusterId = "";
//                        for (String id:clusterIds){
//                            if (!mergedClusterIdList.contains(id)){
//                               finalClusterId +=(id+",");
//                            }
//                        }
//                        finalClusterId += mergedClusterId;
//                        v.setProperty("ClusterId", finalClusterId);
//                        out.collect(v);
//                    }
//                }
//            }
        }
        out.collect(vertex);
    }

    class Sorter implements Comparator<Tuple2<Cluster, Double>> {
        @Override
        public int compare(Tuple2<Cluster, Double> cs1, Tuple2<Cluster, Double> cs2) {
            return cs1.f1.compareTo(cs2.f1) * -1;
        }
    }

    private Collection<String> isMergable (List<Tuple2<Cluster, Double>> Cluster_AssDegree, Vertex Vertex){
        Collection<String> mergingClusterIds = new ArrayList<>();
        String vertexId = Vertex.getId().toString();
        Collection<String> sources = new ArrayList<>();
        sources.add(Vertex.getPropertyValue("type").toString());
        boolean returnFlag = false;
        for (Tuple2<Cluster,Double> elem : Cluster_AssDegree){
            if (elem.f0.hasOverlap(vertexId))
                break;
            for (Vertex v : elem.f0.getVertices()){
                if(!v.getId().toString().equals(vertexId)) {
                    String src = v.getPropertyValue("type").toString();
                    if (sources.contains(src)){
                        returnFlag = true;
                        break;
                    }
                    else
                        sources.add(src);
                }
            }
            if (returnFlag)
                break;
            mergingClusterIds.add(elem.f0.getClusterId());
        }
        if (mergingClusterIds.size() < 2)
            mergingClusterIds.clear();
        return mergingClusterIds;
    }


    private Collection<String> isMergable2 (List<Tuple2<Cluster, Double>> Cluster_AssDegree, Vertex Vertex){
        // to remove isMergingPossible3
        return null;
    }
//        Collection<Cluster> clusters = new ArrayList<>();
//        String vertexId = "";
//        Vertex vertex = null;
//        for (Tuple3<String, Vertex, Cluster> elem : in){
//            vertexId = elem.f0;
//            vertex = elem.f1;
//            clusters.add(elem.f2);
//        }
//        // merge
//        if (isMergable(clusters, vertex)) {
//            String mergedClusterId = merge(clusters);
//            vertex.setProperty("ClusterId", mergedClusterId);
//            out.collect(vertex);
//            for (Cluster c:clusters){
//                for (Vertex v:c.getVertices()){
//                    if(!v.getId().toString().equals(vertexId)) {
//                        v.setProperty("ClusterId", mergedClusterId);
//                        out.collect(v);
//                    }
//                }
//            }
//        }
//        // assign to the most relevant
//        else {
//            String maxClusterId = "";
//            Double maxAssociatioDegree = 0d;
//            for (Cluster c:clusters){
//                Double associationDegree = c.getAssociationDegree4(vertexId);
//                if (associationDegree > maxAssociatioDegree){
//                    maxAssociatioDegree = associationDegree;
//                    maxClusterId = c.getClusterId();
//                }
//            }
//            vertex.setProperty("ClusterId", maxClusterId);
//            out.collect(vertex);
//        }
//    }
//    private boolean isMergable (Collection<Cluster> Clusters, Vertex Vertex){
//        Collection<String> sources = new ArrayList<>();
//        sources.add(Vertex.getPropertyValue("type").toString());
//        String VertexId = Vertex.getId().toString();
//        for (Cluster c:Clusters){
//            for (Vertex v:c.getVertices()){
//                if(!v.getId().toString().equals(VertexId)) {
//                    String src = v.getPropertyValue("type").toString();
//                    if (sources.contains(src))
//                        return false;
//                    sources.add(src);
//                }
//            }
//        }
//        return true;
//    }
//    private String merge (Collection<Cluster> Clusters){
//        String mergedClusterId = "M";
//        for (Cluster c:Clusters){
//            mergedClusterId += c.getClusterId();
//        }
//        return mergedClusterId;
//    }
}





































