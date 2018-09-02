package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.*;


/**
 *
 */
public class resolveReducer4 implements GroupReduceFunction <Tuple3<String, Vertex, Cluster>, Tuple3<Vertex, String, String>> {

    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, Cluster>> in, Collector<Tuple3<Vertex, String, String>> out) throws Exception {
        List<Tuple2<Cluster, Double>> cluster_associationDegree = new ArrayList<>();
        Vertex vertex = null;

        for (Tuple3<String, Vertex, Cluster> elem : in){
//            System.out.println(elem.f2.getAssociationDegree40(elem.f0)+")))))((((((( "+elem.f1.getPropertyValue("recId")+", "+
//                    elem.f2.getClusterId()+", interLinks: "+elem.f2.getInterLinks().size()
//            +" "+", intraLinks: "+elem.f2.getIntraLinks().size());
            cluster_associationDegree.add(Tuple2.of(elem.f2, elem.f2.getAssociationDegree40(elem.f0)));
            vertex = elem.f1;
        }
//        if (vertex.getPropertyValue("recId").toString().equals("s3")) {
//            for (Tuple2<Cluster, Double> elem : cluster_associationDegree) {
//                System.out.println(" **********JJJ***************** " + elem.f0.getClusterId() + "," + elem.f1);
//            }
//        }
        Collections.sort(cluster_associationDegree, new Sorter());
        Collection<String> mergedClusterIdList = isMergable(cluster_associationDegree, vertex);
        String[] clusterIds = vertex.getPropertyValue("ClusterId").toString().split(",");
//       { if (mergedClusterIdList.size()==0) // resolve
            String newClusterId = cluster_associationDegree.get(0).f0.getClusterId();
            for(String id:clusterIds) {
                out.collect(Tuple3.of(vertex, id, newClusterId));
            }
            return;
//        }
//        else {//merge
//            String mergedClusterId="";
//            for (String id:mergedClusterIdList)
//                mergedClusterId+=(","+id);
////            vertex.setProperty("ClusterId", mergedClusterId);
//            //simple imp: merge iff the clusters has no other overlap
////            vertex.setId(GradoopId.fromString("-1"));
//            mergedClusterId = mergedClusterId.substring(1);
//            for (String id:mergedClusterIdList)
//                out.collect(Tuple3.of(null, id, mergedClusterId));
//            for(String id:clusterIds) // The overlapped vertex must be resolved, so should not be owerwritten by other cluster ids
//                out.collect(Tuple3.of(vertex, id, mergedClusterId));
//        }
    }

    class Sorter implements Comparator<Tuple2<Cluster, Double>> {
        @Override
        public int compare(Tuple2<Cluster, Double> cs1, Tuple2<Cluster, Double> cs2) {
            return cs1.f1.compareTo(cs2.f1) * -1;
        }
    }

    private Collection<String> isMergable (List<Tuple2<Cluster, Double>> Cluster_AssDegree, Vertex Vertex){
//        for (Tuple2<Cluster,Double> elem : Cluster_AssDegree) {
//            if (Vertex.getPropertyValue("recId").toString().equals("s3"))
//                System.out.println(" *************************** "+elem.f0.getClusterId()+","+elem.f1);
//        }
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





































