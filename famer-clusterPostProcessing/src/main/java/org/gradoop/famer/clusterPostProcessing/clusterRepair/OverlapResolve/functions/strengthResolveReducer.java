package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class strengthResolveReducer implements GroupReduceFunction<Tuple3<String, Vertex, Cluster>, Tuple3<Vertex, String, String>> {

    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, Cluster>> input, Collector<Tuple3<Vertex, String, String>> output) throws Exception {
        Collection<Cluster> engagingClusters = new ArrayList<>();
        Vertex vertex = null;
        for (Tuple3<String, Vertex, Cluster> elem:input){
            vertex = elem.f1;
                for (Edge e: elem.f2.getIntraLinks()){
                    String vertexId = elem.f0;
                    if (e.getSourceId().toString().equals(vertexId)) {// find the vertex at the other end
                        Vertex otherVertex = elem.f2.getVertex(e.getTargetId());
                        if (!otherVertex.getPropertyValue("ClusterId").toString().contains(",")) {
                            engagingClusters.add(elem.f2);
                            break;
                        }
                    }
                    else if (e.getTargetId().toString().equals(vertexId)) {// find the vertex at the other end
                        Vertex otherVertex = elem.f2.getVertex(e.getSourceId());
                        if (!otherVertex.getPropertyValue("ClusterId").toString().contains(",")) {
                            engagingClusters.add(elem.f2);
                            break;
                        }
                    }
                }
        }
        String[] oldClusterIdList = vertex.getPropertyValue("ClusterId").toString().split(",");
        // state 1 : no strong link to any other vertex
        if (engagingClusters.size() == 0) {// unchanged
            // check if it is unchanged
            for (String id: oldClusterIdList)
                output.collect(Tuple3.of(vertex, id, "Unchanged" ));
            return;
        }
        else if (engagingClusters.size() == 1) {// resolve
            String newClusterId =  engagingClusters.iterator().next().getClusterId();
            for (String id: oldClusterIdList)
                output.collect(Tuple3.of(vertex, id, newClusterId ));
            return;
        }
        else {
            List<String> nonOverlapClusterIds = new ArrayList<>();
            for (Cluster cluster:engagingClusters){
                if (!cluster.hasOverlap())
                    nonOverlapClusterIds.add(cluster.getClusterId());
            }
            String mergedClusterId = "M";
            for (String clusterId:nonOverlapClusterIds){
                mergedClusterId +=clusterId;
            }
            for (String clusterId:nonOverlapClusterIds){
                output.collect(Tuple3.of(null, clusterId, mergedClusterId ));
            }
            if (nonOverlapClusterIds.size() > 0){
                for (String id:oldClusterIdList){ // to do...
                    output.collect(Tuple3.of(vertex, id, mergedClusterId ));
                }
            }
        }


    }
}

