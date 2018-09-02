package org.gradoop.famer.clusterPostProcessing.clusterRepair.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 */
public class consolidateMergedClusterIds implements GroupReduceFunction <Tuple2<Vertex, String>, Tuple2<Vertex, String>>{
    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> values, Collector<Tuple2<Vertex, String>> out) throws Exception {
        Collection<String> oldClusterIds = new ArrayList<>();
        Collection<String> mergingClusterIds = new ArrayList<>();
        Collection<String> singleMergingClusterIds = new ArrayList<>();

        Tuple2<Vertex, String> vertex_gradoopId = new Tuple2<>();
        for (Tuple2<Vertex, String> value: values){
            vertex_gradoopId = value;
            String clusterId = value.f0.getPropertyValue("ClusterId").toString();
            if (clusterId.contains("R,")) {
                clusterId = clusterId.replaceAll("R,","");
                vertex_gradoopId.f0.setProperty("ClusterId", clusterId);
                out.collect(vertex_gradoopId);
                return;
            }
            if (!clusterId.contains("M,")) {
                String[] idList = clusterId.split(",");
                for (String i : idList){
                    if (!oldClusterIds.contains(i))
                        oldClusterIds.add(i);
                }
            }
            else {
                clusterId = sort (clusterId);
                if (!mergingClusterIds.contains(clusterId)) {
                    mergingClusterIds.add(clusterId);
                    String[] idList = clusterId.split(",");
                    for (String i : idList){
                        if (!singleMergingClusterIds.contains(i) && !i.equals("M"))
                            singleMergingClusterIds.add(i);
                    }
                }
            }
        }
        Collection<String> remainingClusterIds = new ArrayList<>();
        for (String id:oldClusterIds){
            if (!singleMergingClusterIds.contains(id) && !remainingClusterIds.contains(id))
                remainingClusterIds.add(id);
        }
        String finalClusterId = "";
        for (String id: mergingClusterIds){
            id = id.replaceAll(",","");
            finalClusterId += (","+id);
        }
        for (String id: remainingClusterIds){
            finalClusterId += (","+id);
        }
        vertex_gradoopId.f0.setProperty("ClusterId", finalClusterId.substring(1));
        out.collect(vertex_gradoopId);
    }

    private String sort (String input){
        String output = "M";
        input = input.replace("M,","");
        String[] inputArray = input.split(",");
        Arrays.sort(inputArray);
        for (int i=0; i< inputArray.length; i++)
            output+= (","+inputArray[i]);
        return output;
    }
}
