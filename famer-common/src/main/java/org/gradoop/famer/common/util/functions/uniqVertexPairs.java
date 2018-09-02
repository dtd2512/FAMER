package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.functions.removeF2Tuple3;

/**
 */
public class uniqVertexPairs {
    private DataSet<Tuple2<Vertex, Vertex>> vertices;
    public uniqVertexPairs (DataSet<Tuple2<Vertex, Vertex>> Vertices) { vertices = Vertices;}
    public DataSet<Tuple2<Vertex, Vertex>> execute(){
        DataSet<Tuple3<Vertex, Vertex, String>> tuple2Vertex_tuple3Vertex_ids = vertices.map(new tuple2Vertex2tuple3Vertex_ids());
        return tuple2Vertex_tuple3Vertex_ids.distinct(2).map(new removeF2Tuple3());
    }
}
