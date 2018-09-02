package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Stack;


/**
 *
 */
public class resolveReducer implements GroupReduceFunction <Tuple3<String, Vertex, Cluster>, Vertex> {
    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, Cluster>> in, Collector<Vertex> out) throws Exception {
        Collection<Tuple2<Cluster,Integer>> cluster_status = new ArrayList<>();
        String source = "";
        Vertex vertex = new Vertex();
        boolean isMerge = true;
        boolean isAssociatedTo1Cluster = false;
        for (Tuple3<String, Vertex, Cluster> elem : in){
            source = elem.f1.getPropertyValue("type").toString();
            vertex = elem.f1;
            Integer hasSourceStatus = elem.f2.hasSource(source);
            if (hasSourceStatus != 0)
                isMerge = false;
            if (hasSourceStatus != 2)
                cluster_status.add(Tuple2.of(elem.f2, hasSourceStatus));
        }
        if (cluster_status.size() == 0) {
            vertex.setProperty("ClusterId", "R,single"+ vertex.getPropertyValue("VertexPriority"));
            out.collect(vertex);
        }
        else if (cluster_status.size() == 1) {
            Tuple2<Cluster, Integer> elem = cluster_status.iterator().next();
            vertex.setProperty("ClusterId","R,"+elem.f0.getClusterId());
            out.collect(vertex);
        }
//        else if (isMerge) {
//            Iterator iterator = cluster_status.iterator();
//            Tuple2<Cluster, Integer> elem = (Tuple2<Cluster, Integer>) iterator.next();
//            String finalClusterId = elem.f0.getClusterId();
//            vertex.setProperty("ClusterId", finalClusterId);
//            out.collect(vertex);
//            while (iterator.hasNext()){
//                elem = (Tuple2<Cluster, Integer>) iterator.next();
//                String clusterClusterId = elem.f0.getClusterId();
//                for (Vertex v : elem.f0.getVertices()) {
//                    if(!v.getId().toString().equals(vertex.getId().toString())) {
//                        String[] clusterIds = v.getPropertyValue("ClusterId").toString().split(",");
//                        Collection<String> newClusterIdsList = new ArrayList<>();
//                        for (String id : clusterIds) {
//                            if (id.equals(clusterClusterId))
//                                id = finalClusterId;
//                            if (!newClusterIdsList.contains(id))
//                                newClusterIdsList.add(id);
//                        }
//                        String newClusterIds = "";
//                        for (String id : newClusterIdsList) {
//                            newClusterIds+= (","+id);
//                        }
//                        v.setProperty("ClusterId", newClusterIds.substring(1));
//                        out.collect(v);
//                    }
//                }
//            }
//        }
        else if (isMerge)
        {
            Cluster[] clusters = new Cluster[cluster_status.size()];
            int  i = 0;
            for (Tuple2<Cluster, Integer> c_s: cluster_status) {
                clusters[i] = c_s.f0;
                i++;
            }
            Collection<Cluster> MergingClusters = isMergePossible(clusters);
            if (MergingClusters.size() > 1) {
                String mergingClusterIds = "M,";
                for (Cluster cluster: MergingClusters){
                    mergingClusterIds += (cluster.getClusterId()+",");
                }
                for (Cluster cluster: MergingClusters){
                    for (Vertex v: cluster.getVertices()){
                        out.collect(v);
                        v.setProperty("ClusterId", mergingClusterIds);
                        out.collect(v);
                    }
                }
            }
            else
                isAssociatedTo1Cluster = true;
        }
        else
            isAssociatedTo1Cluster = true;
        if (isAssociatedTo1Cluster) {
            double maxAssociationDegree = 0;
            String maxClusterId = "";
            String vertexId = vertex.getId().toString();
            Iterator iterator = cluster_status.iterator();
            while (iterator.hasNext()) {
                Cluster cluster = ((Tuple2<Cluster, Integer>)(iterator.next())).f0;
                double associationDegree = cluster.getAssociationDegree(vertexId);
                if (associationDegree > maxAssociationDegree) {
                    maxAssociationDegree = associationDegree;
                    maxClusterId = cluster.getClusterId();
                }
            }
            vertex.setProperty("ClusterId", "R,"+maxClusterId);
            out.collect(vertex);
        }
//        if (isAssociatedTo1Cluster) {
//            Tuple2<Double, Integer> maxAssociationDegree = Tuple2.of(0d,0);
//            String maxClusterId = "";
//            String vertexId = vertex.getId().toString();
//            Iterator iterator = cluster_status.iterator();
//            while (iterator.hasNext()) {
//                Cluster cluster = ((Tuple2<Cluster, Integer>)(iterator.next())).f0;
//                Tuple2<Double, Integer> associationDegree = cluster.getAssociationDegree3(vertexId);
//                if (associationDegree.f1 > maxAssociationDegree.f1) {
//                    maxAssociationDegree = associationDegree;
//                    maxClusterId = cluster.getClusterId();
//                }
//                else if (associationDegree.f0 > maxAssociationDegree.f0) {
//                    maxAssociationDegree = associationDegree;
//                    maxClusterId = cluster.getClusterId();
//                }
//            }
//            vertex.setProperty("ClusterId", maxClusterId);
//            out.collect(vertex);
//        }
    }

    public Collection<Cluster> isMergePossible (Cluster[] Clusters) {
        Collection<Tuple2<String, String>> decisionLinks = new ArrayList<>();
        for (int i =0; i < Clusters.length; i++) {
            for (int j = i+1; j < Clusters.length; j++){
                if (Clusters[i].isMergingPossible (Clusters[j])) {
                    decisionLinks.add(Tuple2.of(Clusters[i].getClusterId(), Clusters[j].getClusterId()));
                }
            }
        }
        Collection<String> mergingClusterIds = decideMergingClusters (decisionLinks);
        Collection<Cluster> out = new ArrayList<>();
        for (int i =0; i < Clusters.length; i++) {
            if (mergingClusterIds.contains(Clusters[i].getClusterId()))
                out.add(Clusters[i]);
        }
        return out;
    }


    // to be repaired
    /* simple version
    public Collection<String> decideMergingClusters (Collection<Tuple2<String,String>> ClusterIds){
        Collection<String> out = new ArrayList<>();
        for (Tuple2<String,String> ids : ClusterIds) {
            if (!out.contains(ids.f0))
                out.add(ids.f0);
            if (!out.contains(ids.f1))
                out.add(ids.f1);
        }
        return out;
    }
    */
    public Collection<String> decideMergingClusters (Collection<Tuple2<String,String>> ClusterIds){
        Collection<String> out = new ArrayList<>();
        if (ClusterIds.size() == 1){
            Tuple2<String, String> ids = ClusterIds.iterator().next();
            out.add(ids.f0);
            out.add(ids.f1);
            return out;
        }
        ClusterIds = sort (ClusterIds);
        for (Tuple2<String,String> ids : ClusterIds) {
            Stack<Tuple2<String,String>> stack  = new Stack<>();
            Collection<Tuple2<String, String>> nextList = new ArrayList<>();
            Collection<Tuple2<String,String>> desiredSequence = new ArrayList<>();
            desiredSequence.add(ids);
            String ending = ids.f1;
            while (true) {
                do {
                    nextList = findNext(ClusterIds, ending);
                    Iterator<Tuple2<String, String>> iterator = nextList.iterator();
                    if (iterator.hasNext()) {
                        Tuple2<String, String> nextItem = nextList.iterator().next();
                        desiredSequence.add(nextItem);
                        ending = nextItem.f1;
                    }
                    while (iterator.hasNext()) {
                        stack.push(iterator.next());
                    }
                } while (nextList.size() != 0);

                if (checkConnectivity(desiredSequence, ClusterIds)) {
                    if (desiredSequence.size() + 1 > out.size())
                        out = LinkSequence2ClusterIdSequence(desiredSequence);
                }
                if (!stack.empty()) {
                    Tuple2<String, String> top = stack.pop();
                    desiredSequence = removeExtra(desiredSequence, top.f0);
                    desiredSequence.add(top);
                }
                else
                    break;
            }
        }
        return out;
    }

    private Collection<Tuple2<String,String>> sort (Collection<Tuple2<String,String>> inputCollection){
        Collection<Tuple2<String,String>> outputCollection = new ArrayList<>();
        for (Tuple2<String,String> input: inputCollection){
            String first, second;
            if (input.f0.compareTo(input.f1)<0){
                first = input.f0;
                second = input.f1;
            }
            else {
                first = input.f1;
                second = input.f0;
            }
            outputCollection.add(Tuple2.of(first, second));
        }
        return outputCollection;
    }

    private Collection<Tuple2<String, String>> findNext (Collection<Tuple2<String,String>> ClusterIds , String Id){
        Collection<Tuple2<String, String>> next = new ArrayList<>();
        for (Tuple2<String,String> clusterId: ClusterIds){
            if (clusterId.f0.equals(Id))
                next.add(clusterId);
        }
        return next;
    }

    private Boolean checkConnectivity (Collection<Tuple2<String,String>> DesiredSequence, Collection<Tuple2<String,String>> ClusterIds){
        Collection<String> clusterIds = LinkSequence2ClusterIdSequence(DesiredSequence);
        String[] clusterIdsArray = clusterIds.toArray(new String[clusterIds.size()]);
        for (int i=0; i< clusterIdsArray.length; i++){
            for (int j=i+1; j< clusterIdsArray.length; j++){
                if (!ClusterIds.contains(Tuple2.of(clusterIdsArray[i],clusterIdsArray[j]))
                        && !ClusterIds.contains(Tuple2.of(clusterIdsArray[j],clusterIdsArray[i])))
                    return false;
            }
        }

        return true;
    }

    private Collection<String> LinkSequence2ClusterIdSequence (Collection<Tuple2<String,String>> input){
        Collection<String> out = new ArrayList<>();
        for (Tuple2<String,String> ids: input){
            if (!out.contains(ids.f0))
                out.add(ids.f0);
            if (!out.contains(ids.f1))
                out.add(ids.f1);
        }
        return out;
    }

    private Collection<Tuple2<String, String>> removeExtra (Collection<Tuple2<String, String>> DesiredSequence, String id){
        Collection<Tuple2<String, String>> out = new ArrayList<>();
        for (Tuple2<String, String> ids: DesiredSequence){
            if (!ids.f0.equals(id))
                out.add(ids);
            else
                break;
        }
        return out;
    }
}



































