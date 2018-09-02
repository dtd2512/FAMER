package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class Vertex2VertexData implements FlatMapFunction<Vertex, Tuple3<String, String, String>> {
    private String Label;
    private String clusteridLabel;
    private String typeLabel;
    public Vertex2VertexData (String vertexIdLabel, String ClusterIDLabel, String TypeLabel){
        Label = vertexIdLabel;
        clusteridLabel = ClusterIDLabel;
        typeLabel = TypeLabel;
    }
    public void flatMap(Vertex in, Collector<Tuple3<String, String, String>> out) {
        if (in.getPropertyValue(clusteridLabel).toString().equals("") || in.getPropertyValue("ClusterId").toString().contains(",")) {
            String[] clusterIds = null;
            if (in.getPropertyValue(clusteridLabel).toString().equals("")) {
                clusterIds = in.getPropertyValue(clusteridLabel).toString().split(",");
            }
            else {
                clusterIds = in.getPropertyValue(clusteridLabel).toString().split(",");

            }
            for (int i = 0; i < clusterIds.length; i++) {
                out.collect(Tuple3.of(in.getPropertyValue(Label).toString(), clusterIds[i], in.getPropertyValue(typeLabel).toString()));
            }
        } else {
            out.collect(Tuple3.of(in.getPropertyValue(Label).toString(), in.getPropertyValue(clusteridLabel).toString(),
                    in.getPropertyValue(typeLabel).toString()));
        }
    }
}
