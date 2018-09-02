package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.dataStructures.ClusterID;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.dataStructures.ClusterIDList;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.*;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.consolidateMergedClusterIds;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.vertexVsClusterJoin;
import org.gradoop.famer.common.functions.*;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import scala.Int;

import java.io.Serializable;
import java.util.*;

/**
 */
public class overlapResolveOptimized3 implements UnaryGraphToGraphOperator {
    public enum Method {JOIN, GROUPBY}
    private Method method;
    public overlapResolveOptimized3 (Method computingMethod) {method = computingMethod;}
    @Override
    public LogicalGraph execute(LogicalGraph input) {

        DataSet<Vertex> vertices = input.getVertices();
        DataSet<Tuple2<Vertex, String>> vertex_clusterID = vertices.flatMap(new vertex2vertex_clusterId(true));
        DataSet<Tuple4<Vertex, String, String, Integer>> vertex_vID_clusterID_clusterSize = vertex_clusterID.groupBy(1).reduceGroup(new getClusterSize());
        vertices = vertex_vID_clusterID_clusterSize.groupBy(1).reduceGroup(new func2());
        DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> currentList = new link2link_srcVertex_trgtVertex(vertices, input.getEdges()).execute().flatMap(new func1());


//        // find vertices that must be resolved in this iteration
//        DataSet<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>> curList_OvrlapDgree = currentList.map(new getOverlapDegree());
//
//        DataSet<Tuple1<Integer>> minOvrlapDgree = curList_OvrlapDgree.minBy(6).map(new f4());
//        DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> toBeResolvedList = curList_OvrlapDgree.join(minOvrlapDgree).where(6).equalTo(0).with(new toBeresolvedJoin());
//        // update currentList
//        currentList = new minus (currentList, toBeResolvedList).execute();
//        // resolve and update
////        DataSet<Tuple2<Vertex, String>> vertex_oldClusterID = toBeResolvedList.groupBy(1).reduceGroup(new resolve());









        /*Start Bulk Iteration*/

        // find vertices that must be resolved in this iteration

//
long x = -1l;
do{
        DataSet<Tuple3<Vertex, String, String>> vertex_vID_clusterID = vertices.flatMap(new vertex2vertex_ID_clusterId());


        DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> test = vertices.map(new MapFunction<Vertex, Tuple6<Vertex, String, Vertex, String, Double, String>>() {
            @Override
            public Tuple6<Vertex, String, Vertex, String, Double, String> map(Vertex vertex) throws Exception {
                return Tuple6.of(vertex, "", vertex, "", -1d, "");
            }
        });
        currentList = currentList.union(test).flatMap(new FlatMapFunction<Tuple6<Vertex, String, Vertex, String, Double, String>, Tuple6<Vertex, String, Vertex, String, Double, String>>() {
            @Override
            public void flatMap(Tuple6<Vertex, String, Vertex, String, Double, String> in, Collector<Tuple6<Vertex, String, Vertex, String, Double, String>> collector) throws Exception {
                if (!in.f1.equals(""))
                    collector.collect(in);
            }
        });


        DataSet<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>> curList_OvrlapDgree = currentList.map(new getOverlapDegree());


        DataSet<Tuple1<Integer>> minOvrlapDgree = curList_OvrlapDgree.minBy(6).map(new f4());
        DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> toBeResolvedList = curList_OvrlapDgree.join(minOvrlapDgree).where(6).equalTo(0).with(new toBeresolvedJoin());

        // update currentList
        currentList = new minus(currentList, toBeResolvedList).execute();
        // resolve and update
        DataSet<Tuple2<Vertex, String>> vertex_oldClusterID = toBeResolvedList.groupBy(1).reduceGroup(new resolve());


        vertices = vertex_oldClusterID.rightOuterJoin(vertex_vID_clusterID).where(1).equalTo(2).with(new join1()).groupBy(1).reduceGroup(new unify());
        vertex_clusterID = vertices.flatMap(new vertex2vertex_clusterId(true));
        vertex_vID_clusterID_clusterSize = vertex_clusterID.groupBy(1).reduceGroup(new getClusterSize());
        vertices = vertex_vID_clusterID_clusterSize.groupBy(1).reduceGroup(new func2());
        DataSet<Tuple2<Vertex, String>> updated = vertices.flatMap(new getUpdated()).map(new removeUpdatedProperty()).map(new vertex2vertex_gradoopId());
        vertices = vertices.map(new removeUpdatedProperty());
        currentList = currentList.leftOuterJoin(updated).where(1).equalTo(1).with(new updateLeftSideJoin());
        currentList = currentList.leftOuterJoin(updated).where(3).equalTo(1).with(new updateRightSideJoin());


        DataSet<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>> remainingToBeResolved = currentList.flatMap(new getOverlapDegree2());
    try {
        x = remainingToBeResolved.count();
    } catch (Exception e) {
        e.printStackTrace();
    }


}while (x!=0);


        input = input.getConfig().getLogicalGraphFactory().fromDataSets(vertices, input.getEdges());
//        input = input.callForGraph(new RemoveInterClustersLinks());
        return input;
    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    private class func1 implements FlatMapFunction<Tuple3<Edge, Vertex, Vertex>, Tuple6<Vertex, String, Vertex, String, Double, String>> {
        @Override
        public void flatMap(Tuple3<Edge, Vertex, Vertex> input, Collector<Tuple6<Vertex, String, Vertex, String, Double, String>> output) throws Exception {
            String[] f1Clusters = input.f1.getPropertyValue("ClusterId").toString().split(",");
            String[] f2Clusters = input.f2.getPropertyValue("ClusterId").toString().split(",");
            if (f1Clusters.length > 1) {
                if (hasIntersection(f1Clusters, f2Clusters))
                    output.collect(Tuple6.of(input.f1, input.f1.getId().toString(), input.f2, input.f2.getId().toString(), Double.parseDouble(input.f0.getPropertyValue("value").toString()), input.f0.getId().toString()));
            }
            if (f2Clusters.length > 1) {
                if (hasIntersection(f1Clusters, f2Clusters))
                    output.collect(Tuple6.of(input.f2, input.f2.getId().toString(), input.f1, input.f1.getId().toString(), Double.parseDouble(input.f0.getPropertyValue("value").toString()), input.f0.getId().toString()));
            }
        }

        private boolean hasIntersection(String[] f1Clusters, String[] f2Clusters) {
            for (int i=0; i<f1Clusters.length; i++){
                for (int j=0; j<f2Clusters.length; j++){
                    if (f1Clusters[i].equals(f2Clusters[j]))
                        return true;
                }
            }
            return false;
        }
    }
    private class getClusterSize implements GroupReduceFunction<Tuple2<Vertex, String>, Tuple4<Vertex, String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<Vertex, String>> input, Collector<Tuple4<Vertex, String, String, Integer>> output) throws Exception {
            String clusterId = "";
            Integer cnt = 0;
            Collection<Vertex> vertices = new ArrayList<>();
            for (Tuple2<Vertex, String> in:input){
                vertices.add(in.f0);
                clusterId = in.f1;
                cnt++;
            }
            for (Vertex vertex:vertices)
                output.collect(Tuple4.of(vertex, vertex.getId().toString(), clusterId, cnt));
        }
    }

    private class func2 implements GroupReduceFunction<Tuple4<Vertex, String, String, Integer>, Vertex> {
        @Override
        public void reduce(Iterable<Tuple4<Vertex, String, String, Integer>> input, Collector<Vertex> output) throws Exception {

            String clusterIds = "";
            String clusterSizes = "";
            Vertex vertex = null;
            for (Tuple4<Vertex, String, String, Integer> in :input){
                vertex = in.f0;
                clusterIds += "," +in.f2;
                clusterSizes += "," +in.f3;
            }
            vertex.setProperty("ClusterId", clusterIds.substring(1));
            vertex.setProperty("Sizes", clusterSizes.substring(1));
            output.collect(vertex);
        }
    }

    private class getOverlapDegree implements MapFunction<Tuple6<Vertex, String, Vertex, String, Double, String>,
            Tuple7<Vertex, String, Vertex, String, Double, String, Integer>> {
        @Override
        public Tuple7<Vertex, String, Vertex, String, Double, String, Integer> map(Tuple6<Vertex, String, Vertex, String, Double, String> input) throws Exception {
            Integer clusterSize = input.f0.getPropertyValue("ClusterId").toString().split(",").length;
            return Tuple7.of(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, clusterSize);
        }
    }

    private class f3 implements MapFunction<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>, Tuple6<Vertex, String, Vertex, String, Double, String>> {
        @Override
        public Tuple6<Vertex, String, Vertex, String, Double, String> map(Tuple7<Vertex, String, Vertex, String, Double, String, Integer> in) throws Exception {
            return Tuple6.of(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5);
        }
    }

    private class resolve implements GroupReduceFunction<Tuple6<Vertex, String, Vertex, String, Double, String>, Tuple2<Vertex, String>> {
        @Override
        public void reduce(Iterable<Tuple6<Vertex, String, Vertex, String, Double, String>> input, Collector<Tuple2<Vertex, String>> output) throws Exception {
            HashMap<String, Tuple2<Double, Integer>> clusterId_sumSim_size = new HashMap<>();
            Vertex vertex = null;
            String[] v0Clusters = null ;
            for (Tuple6<Vertex, String, Vertex, String, Double, String> in:input){
                vertex = in.f0;
                v0Clusters = in.f0.getPropertyValue("ClusterId").toString().split(",");
                String[] v1Clusters = in.f2.getPropertyValue("ClusterId").toString().split(",");
                if (v1Clusters.length == 1 ){
                    if (clusterId_sumSim_size.containsKey(v1Clusters[0])) {
                        Tuple2<Double, Integer> item = clusterId_sumSim_size.remove(v1Clusters[0]);
                        item.f0 += in.f4;
                        clusterId_sumSim_size.put(v1Clusters[0], item);
                    }
                    else
                        clusterId_sumSim_size.put(v1Clusters[0], Tuple2.of(in.f4, Integer.parseInt(in.f2.getPropertyValue("Sizes").toString().split(",")[0])));
                }
            }
            for (Map.Entry<String, Tuple2<Double, Integer>> entry : clusterId_sumSim_size.entrySet()) {
                entry.setValue(Tuple2.of(entry.getValue().f0/entry.getValue().f1, entry.getValue().f1));
            }
            Double max = 0d;
            String maxClusterId = "";
            for (Map.Entry<String, Tuple2<Double, Integer>> entry : clusterId_sumSim_size.entrySet()) {
                if (entry.getValue().f0 > max){
                    max = entry.getValue().f0;
                    maxClusterId = entry.getKey();
                }
            }
            vertex.setProperty("ClusterId", maxClusterId);
            for (String clsId: v0Clusters) {
                output.collect(Tuple2.of(vertex, clsId));
            }
        }
    }

    private class vertex2vertex_ID_clusterId implements FlatMapFunction<Vertex, Tuple3<Vertex, String, String>> {
        @Override
        public void flatMap(Vertex vertex , Collector<Tuple3<Vertex, String, String>> output) throws Exception {
            String[] clusterIds= vertex.getPropertyValue("ClusterId").toString().split(",");
            for (String id: clusterIds)
                output.collect(Tuple3.of(vertex, vertex.getId().toString(), id));
        }
    }

    private class join1 implements JoinFunction<Tuple2<Vertex, String>, Tuple3<Vertex, String, String>, Tuple2<Vertex, String>> {
        @Override
        public Tuple2<Vertex, String> join(Tuple2<Vertex, String> vertex_oldClsID, Tuple3<Vertex, String, String> vertex_vID_oldClsID) throws Exception {
            if (vertex_oldClsID!= null && vertex_oldClsID.f0.getId().toString().equals(vertex_vID_oldClsID.f0.getId().toString())) {
                Vertex vertex = vertex_oldClsID.f0;
                vertex.setProperty("updated", true);
                return Tuple2.of(vertex, vertex.getId().toString());
            }
            else {
                Vertex vertex = vertex_vID_oldClsID.f0;
                vertex.setProperty("updated",false);
                return Tuple2.of(vertex, vertex.getId().toString());
            }
        }
    }


    private class unify implements GroupReduceFunction<Tuple2<Vertex, String>, Vertex> {
        @Override
        public void reduce(Iterable<Tuple2<Vertex, String>> input, Collector<Vertex> output) throws Exception {
            Vertex vertex = null;
            for (Tuple2<Vertex, String> in :input){
                if (Boolean.parseBoolean(in.f0.getPropertyValue("updated").toString())) {
                    output.collect(in.f0);
                    return;
                }
                vertex = in.f0;
            }
            output.collect(vertex);
        }
    }

    private class getUpdated implements FlatMapFunction<Vertex, Vertex> {
        @Override
        public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
            if (Boolean.parseBoolean(vertex.getPropertyValue("updated").toString()))
                collector.collect(vertex);
        }
    }

    private class updateLeftSideJoin implements JoinFunction<Tuple6<Vertex, String, Vertex, String, Double, String>, Tuple2<Vertex, String>, Tuple6<Vertex, String, Vertex, String, Double, String>> {
        @Override
        public Tuple6<Vertex, String, Vertex, String, Double, String> join(Tuple6<Vertex, String, Vertex, String, Double, String> curItem, Tuple2<Vertex, String> updatedItem) throws Exception {
            if (updatedItem != null) {
                return Tuple6.of(updatedItem.f0, updatedItem.f1, curItem.f2, curItem.f3, curItem.f4, curItem.f5);
            }
            else
                return curItem;
        }
    }
    private class updateRightSideJoin implements JoinFunction<Tuple6<Vertex, String, Vertex, String, Double, String>, Tuple2<Vertex, String>, Tuple6<Vertex, String, Vertex, String, Double, String>> {
        @Override
        public Tuple6<Vertex, String, Vertex, String, Double, String> join(Tuple6<Vertex, String, Vertex, String, Double, String> curItem, Tuple2<Vertex, String> updatedItem) throws Exception {
            if (updatedItem != null) {
                return Tuple6.of(curItem.f0, curItem.f1, updatedItem.f0, updatedItem.f1, curItem.f4, curItem.f5);
            }
            else
                return curItem;
        }
    }

    private class removeUpdatedProperty implements MapFunction<Vertex, Vertex> {
        @Override
        public Vertex map(Vertex vertex) throws Exception {
            vertex.removeProperty("updated");
            return vertex;
        }
    }

    private class getOverlapDegree2 implements FlatMapFunction<Tuple6<Vertex, String, Vertex, String, Double, String>, Tuple7<Vertex, String, Vertex, String, Double, String, Integer>> {
        @Override
        public void flatMap(Tuple6<Vertex, String, Vertex, String, Double, String> input, Collector<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>> output) throws Exception {
            Integer clusterSize = input.f0.getPropertyValue("ClusterId").toString().split(",").length;
            if (clusterSize > 1)
                output.collect(Tuple7.of(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, clusterSize));
        }
    }

    private class toBeresolvedJoin implements JoinFunction<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>, Tuple1<Integer>, Tuple6<Vertex, String, Vertex, String, Double, String>> {
        @Override
        public Tuple6<Vertex, String, Vertex, String, Double, String> join(Tuple7<Vertex, String, Vertex, String, Double, String, Integer> in1, Tuple1<Integer> in2) throws Exception {
            return Tuple6.of(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, in1.f5);
        }
    }

    private class f4 implements MapFunction<Tuple7<Vertex, String, Vertex, String, Double, String, Integer>, Tuple1<Integer>> {
        @Override
        public Tuple1<Integer> map(Tuple7<Vertex, String, Vertex, String, Double, String, Integer> input) throws Exception {
            return Tuple1.of(input.f6);
        }
    }

//    private class convertClusterIds implements MapFunction<Vertex, Vertex> {
//        @Override
//        public Vertex map(Vertex vertex) throws Exception {
//            Collection<Tuple2<String, Integer>> clusters = (Collection<Tuple2<String, Integer>>) vertex.getPropertyValue("ClusterId");
//            vertex.setProperty("ClusterId", clusters.iterator().next().f0);
//            return vertex;
//        }
//    }
}

















