package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.OVFunc;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.resolveReducer3;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.*;

/**
 */
public class MergeResolver implements GroupReduceFunction<Tuple3<String, Vertex, Cluster>, Tuple3<Vertex, String, String>> {
    private ResolveIteration iteration;
    public MergeResolver(ResolveIteration resolveIteration){iteration = resolveIteration;}
    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, Cluster>> input, Collector<Tuple3<Vertex, String, String>> output) throws Exception {

        Collection<Cluster> engagingClusters = new ArrayList<>();
        Integer relatedVerticesSize = 0;
        Vertex vertex = null;
        for (Tuple3<String, Vertex, Cluster> in:input){
            Collection<Vertex> relatedVertices = in.f2.getRelatedVertices(in.f1.getId());
            relatedVerticesSize += relatedVertices.size();
            vertex = in.f1;
            if (!isAllOverlap(relatedVertices))
                engagingClusters.add(in.f2);
            if(iteration.equals(ResolveIteration.ITERATION2) && in.f1.getPropertyValue("recId").toString().equals("03429421s1"))
                System.out.println("GHGHGHGHGHG "+in.f1.getPropertyValue("ClusterId")+" "+in.f2.getClusterId());
        }

        String[] clusterIds = vertex.getPropertyValue("ClusterId").toString().split(",");

        String newClusterId = "";
        String mergedClusterId = "";
        Collection<String> mergableClusters = new ArrayList<>();

        // state 1: not connected to any vertex
        if (relatedVerticesSize == 0){
            // singletone
            newClusterId = "s"+vertex.getPropertyValue("VertexPriority").toString();
        }
        // state 2: just connected to overlapped vertices
        else if (engagingClusters.size() == 0){
            // iteration 1 unchanged
            // iteration 2 : singleton
            if (iteration.equals(ResolveIteration.ITERATION2)) {
                newClusterId = "s" + vertex.getPropertyValue("VertexPriority").toString();
            }
        }
        // state 3: just connected to 1 cluster
        else if (engagingClusters.size() == 1){
            // resolve
            newClusterId = engagingClusters.iterator().next().getClusterId();
        }
        // state 4: connected to more than 1 clusters
        else {
//            if(iteration.equals(ResolveIteration.ITERATION2)) {
//                for (Cluster c:engagingClusters){
////                    for (Vertex v: c.getVertices())
//                        System.out.println(c.getVertices().size() + "****)))((((");
//                }
//            }
            Collection<Cluster> candidateClusters = findCandidates(engagingClusters, vertex.getId().toString()); // clusters with no other overlapped vertex except "vertex"
            if (candidateClusters.size() > 1) {

                mergableClusters = findMergeables(candidateClusters, vertex);


//                    for (Cluster c:candidateClusters){
//    //                    for (Vertex v: c.getVertices())
//                            System.out.println(c.getVertices().size() + "****)))((((");
//                    }

            }
//            candidateClusters.clear();
            if (iteration.equals(ResolveIteration.ITERATION1)) {
                if (mergableClusters.size() > 0){ // B: merge
                    engagingClusters.clear();
                    //merge
                    mergedClusterId = "M";
                    for (String id :mergableClusters)
                        mergedClusterId += id;
                }
                else if (engagingClusters.size() < clusterIds.length){ // C: update to lower degree
                    // update to lower degree
                    for (Cluster cluster: engagingClusters)
                        newClusterId += (","+cluster.getClusterId());
                    newClusterId = newClusterId.substring(1);
                }

            }
            else { // ITERATION2
                if (mergableClusters.size() > 0){ // B: merge: modify engagingClusters
//                    if(iteration.equals(ResolveIteration.ITERATION2)) {
                    mergedClusterId = "M";
                    for (String id :mergableClusters)
                        mergedClusterId += id;
                    Cluster mergedCluster = merge(engagingClusters, mergableClusters, vertex, mergedClusterId);
                    engagingClusters = minusClusters(engagingClusters, mergableClusters);
                    engagingClusters.add(mergedCluster);
                }
                Double maxDegree = -1d;
                for (Cluster cluster:engagingClusters) {
                    Double assDegree = cluster.getAssociationDegree40(vertex.getId().toString());
                    if (assDegree > maxDegree) {
                        maxDegree = assDegree;
                        newClusterId = cluster.getClusterId();
                    }
                }
                mergedClusterId = "";
            }
        }
Collection<String> test = new ArrayList<>();
        Collection<String> en = new ArrayList<>();
        for (Cluster s:engagingClusters)
            en.add(s.getClusterId());
        for(String s:clusterIds){
            if (!en.contains(s))
                test.add(s);
        }
        if(iteration.equals(ResolveIteration.ITERATION2)){
            System.out.println(engagingClusters.size()+"uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu "+test.size());

        }

        if (!mergedClusterId.equals("")) {
            List<String> remainingIds = minus (clusterIds, mergableClusters);
            for (String id : remainingIds)
                newClusterId += (id +",");
            newClusterId += mergedClusterId;
            for (String id : remainingIds){
                output.collect(Tuple3.of(vertex, id, newClusterId));
            }
            for (String id : mergableClusters)
                output.collect(Tuple3.of(null, id, mergedClusterId));
        }
        else if (!newClusterId.equals("")) {
            for (String id : clusterIds) {
                if(iteration.equals(ResolveIteration.ITERATION2) && vertex.getPropertyValue("recId").toString().equals("03429421s1"))
                    System.out.println("YYYYYYYYYYYY "+id+" **** "+newClusterId);
                output.collect(Tuple3.of(vertex, id, newClusterId));
            }
            if (iteration.equals(ResolveIteration.ITERATION2)) {
                for (String s:test)
                    output.collect(Tuple3.of(vertex, s, newClusterId));
            }

        }
    }




    private boolean isAllOverlap(Collection<Vertex> relatedVertices) {
        for (Vertex v: relatedVertices){
            if (!v.getPropertyValue("ClusterId").toString().contains(","))
                return false;
        }
        return true;
    }
    private Collection<Cluster> findCandidates (Collection<Cluster> clusters, String vertexId){
        Collection <Cluster> candidateClusters = new ArrayList<>();
        for (Cluster cluster : clusters){
            if (!cluster.hasOverlap(vertexId) && cluster.isConsistent())
                candidateClusters.add(cluster);
        }
        return candidateClusters;
    }


    private Collection<String> findMergeables (Collection<Cluster> clusters, Vertex vertex){

        // find and sort based on ass degrees
        List<Tuple2<Cluster, Double>> cluster_assDegree = new ArrayList<>();
        String vertexId = vertex.getId().toString();
        for (Cluster cluster:clusters){
            cluster_assDegree.add(Tuple2.of(cluster, cluster.getAssociationDegree40(vertexId)));
        }
        clusters.clear();
        Collections.sort(cluster_assDegree, new ClusterSorter());


        // find mergeables
        Collection<String> mergableClusterIds = new ArrayList<>();
        Collection<String> sources = new ArrayList<>();
        for (Tuple2<Cluster,Double> cluster_degree:cluster_assDegree){
            cluster_degree.f0.removeFromVertices(vertex);
            Collection<String> newSources = cluster_degree.f0.getSources();
            if (!hasIntersection(sources, newSources)){
                sources.addAll(newSources);
                mergableClusterIds.add(cluster_degree.f0.getClusterId());
            }
            else
                break;
        }
        if (mergableClusterIds.size() == 1)
            mergableClusterIds.clear();
        return mergableClusterIds;
    }
    private Boolean hasIntersection (Collection<String> l1, Collection<String> l2){
        for (String s:l2){
            if (l1.contains(s))
                return true;
        }
        return false;
    }
    private List<String> minus (String[] first, Collection<String> second){
        List<String> out = new ArrayList<>();
        for(String s1: first){
            if(!second.contains(s1))
                out.add(s1);
        }
        return out;
    }
    private Cluster merge(Collection<Cluster> engagingClusters, Collection<String> mergableClusters, Vertex vertex, String mergedClusterId){
        Cluster output = null;
        Collection<Cluster> mergables = new ArrayList<>();
        for (Cluster cluster:engagingClusters){
            if (mergableClusters.contains(cluster.getClusterId())) {
                cluster.removeFromVertices(vertex);
                mergables.add(cluster);
            }
        }
        engagingClusters.clear();
        Collection<Vertex> vertices = new ArrayList<>();
        Collection<Edge> intraLinks = new ArrayList<>();

        String[] clusterIds = vertex.getPropertyValue("ClusterId").toString().split(",");
        String clusterId = "";
        for (String id:clusterIds){
            if (!mergableClusters.contains(id))
                clusterId+= (id +",");
        }
        clusterId += mergedClusterId;
        vertex.setProperty("ClusterId", clusterId);
        vertices.add(vertex);
        for (Cluster cluster:mergables){
            for(Vertex v: cluster.getVertices()){
                v.setProperty("ClusterId", mergedClusterId);
                vertices.add(v);
            }
            for(Edge e: cluster.getIntraLinks()){
                intraLinks.add(e);
            }

        }
        output = new Cluster(vertices, intraLinks, null, mergedClusterId, "");
        return output;
    }
    private Collection<Cluster> minusClusters(Collection<Cluster> first, Collection<String> second){
        Collection<Cluster> output = new ArrayList<>();
        for (Cluster cluster: first){
            if (!second.contains(cluster.getClusterId()))
                output.add(cluster);
        }
        return output;
    }

}
