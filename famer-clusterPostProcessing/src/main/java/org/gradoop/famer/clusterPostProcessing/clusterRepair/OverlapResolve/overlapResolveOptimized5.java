package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.*;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.*;
import org.gradoop.famer.common.functions.*;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.*;


public class overlapResolveOptimized5 implements UnaryGraphToGraphOperator {
    @Override
    public String getName() {
        return "overlapResolveBulkIteration";
    }
    private Integer iteration;
    private Boolean merge;
    public overlapResolveOptimized5( Boolean isMerge){iteration = Integer.MAX_VALUE; merge = isMerge;}

    @Override
    public LogicalGraph execute(LogicalGraph clusteredGraph) {


        ClusterCollection cc = new ClusterCollection(clusteredGraph);
        DataSet<Cluster> clusters = cc.getClusterCollection();
//        try {
//            for (Cluster c:clusters.collect()){
//                if (c.getClusterId().equals("15404"))
//                    for (Vertex v: c.getVertices()){
//                        System.out.println(v.getPropertyValue("recId")+", "+ v.getPropertyValue("ClusterId"));
//                    }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

//        try {
//            for(Cluster c:clusters.collect()){
//                System.out.println(c.getClusterId());
//                for (Vertex v: c.getVertices())
//                    System.out.print(v.getPropertyValue("recId")+", ");
//                System.out.println();
//                for (Edge l: c.getInterLinks())
//                    System.out.println(l.getId()+"-"+l.getPropertyValue("value"));
//                System.out.println();
//                for (Edge l: c.getIntraLinks())
//                    System.out.println(l.getId()+"-"+l.getPropertyValue("value"));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return clusteredGraph;

//
        IterativeDataSet<Cluster> initial = clusters.iterate(iteration);
        DataSet<Cluster> iteration = initial;


        /* start: bulk iteration */
        DataSet<Tuple2<Cluster, String>> cluster_clusterId = iteration.map(new cluster2cluster_clusterId());
        DataSet<Vertex> vertices = iteration.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());



        // find vertices that must be resolved in this iteration
        DataSet<Tuple2<Vertex, Integer>> vertex_overlapLength = vertices.flatMap(new overlapLength());
        DataSet<Vertex> toBeResolvedVertices = vertex_overlapLength.minBy(1).join(vertex_overlapLength).where(1).equalTo(1).with(new joinGetSecond()).map(new getF0Tuple2());

        // resolve
        DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
        DataSet<Tuple2<Vertex, Cluster>> toBeResolvedVertex_cluster = toBeResolvedVertex_clusterId.
                join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin());
        DataSet<Tuple3<Vertex, String, String>> updatedVertices = null;
        if (merge)
            updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
                .groupBy(0).reduceGroup(new resolveReducer3());
        else
            updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
                    .groupBy(0).reduceGroup(new resolveReducer4());


        DataSet<Tuple3<Cluster, String, String>> updated = updatedVertices.groupBy(1).reduceGroup(new integrateVertices());

        // update vertex set
        iteration = cluster_clusterId.leftOuterJoin(updated).where(1).equalTo(1).with(new updateJoin2());
        iteration = iteration.map(new cluster2cluster_clusterId()).groupBy(1).reduceGroup(new updateClusters());
        vertices = iteration.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());



        DataSet<Vertex> remainingToBeResolvedVertices = vertices.flatMap(new filterOverlappedVertices());
        DataSet<Cluster> result = initial.closeWith(iteration, remainingToBeResolvedVertices);

        cc.setClusterCollection(result);

        return cc.toLogicalGraph(clusteredGraph.getConfig());

    }

    private class updateJoin implements JoinFunction<Tuple2<Cluster, String>, Tuple3<Vertex, String, String>, Cluster> {
        @Override
        public Cluster join(Tuple2<Cluster, String> in1, Tuple3<Vertex, String, String> in2) throws Exception {

            Cluster cluster = in1.f0;

            if (in2 == null)
                return cluster;


            else {
                Vertex vertex = in2.f0;
                String oldClusterId = in2.f1;
                String newClusterId = in2.f2;
                String clusterClusterId = in1.f1;
                if (vertex != null){ // resolve case
                    cluster.removeFromVertices(vertex);
                    if (clusterClusterId.equals(newClusterId))// if clusterId == resolved clusterid
                    {
                        vertex.setProperty("ClusterId", newClusterId);
                        cluster.addToVertices(vertex);
                    }


                    return cluster;
                }
                else { // merge case
                    String mergedClusterId = "";
                    for(Vertex v:cluster.getVertices()) {
                        String[] oldClusterIds = newClusterId.split(",");
                        mergedClusterId = "M"+newClusterId.replaceAll(",","");
                        // it is the simpe merge, where clustrs are merged when none of them has overlap. o.w. newId should be considered

//                        String[] clusterIds = v.getPropertyValue("ClusterId").toString().split(",");
//                        String newId = minusStringArray(clusterIds, oldClusterIds);
//                        if (newId.equals(""))
//                            newId = mergedClusterId;
//                        else
//                            newId += (","+mergedClusterId);
                        v.setProperty("ClusterId", mergedClusterId);
                    }
                    cluster.setComponentId(mergedClusterId);
                    return cluster;
                }
            }
        }
        private String minusStringArray(String[] array1, String[] array2){
            String output= "";
            List<String> list2 = new ArrayList(array2.length);
            list2.addAll(Arrays.asList(array2));

            for (String s:array1){
                if (!list2.contains(s))
                    output += (","+s);
            }
            if (output.equals(""))
                return "";
            return output.substring(1);
        }
    }



    private class updateJoin1 implements JoinFunction<Tuple2<Cluster, String>, Tuple3<Cluster, String, String>, Cluster> {
        @Override
        public Cluster join(Tuple2<Cluster, String> in1, Tuple3<Cluster, String, String> in2) throws Exception {
            Cluster cluster = in1.f0;
            if (in2 == null)
                return cluster;
            else {
                Cluster newVertices = in2.f0;
                String oldClusterId = in2.f1;
                String newClusterId = in2.f2;
                String clusterClusterId = in1.f1;
                if (in2.f2.equals("")) {//resolve case
                    for (Vertex v : newVertices.getVertices()) {
                        String newVertexClusterId = v.getPropertyValue("ClusterId").toString();
                        cluster.removeFromVertices(v.getId());
                        if (newVertexClusterId.equals(clusterClusterId)) {
                            cluster.addToVertices(v);
                        }
                    }
                }
                else {//merge case
                    for (Vertex v : cluster.getVertices()) {
                        v.setProperty("ClusterId", newClusterId);
                    }
                }
                return cluster;
            }
        }
    }


    private class updateJoin2 implements JoinFunction<Tuple2<Cluster, String>, Tuple3<Cluster, String, String>, Cluster> {
        @Override
        public Cluster join(Tuple2<Cluster, String> in1, Tuple3<Cluster, String, String> in2) throws Exception {
            Cluster cluster = in1.f0;
            if (in2 == null)
                return cluster;
            else {
                Cluster newVertices = in2.f0;
                String oldClusterId = in2.f1;
                String newClusterId = in2.f2;
                String clusterClusterId = in1.f1;
                if (in2.f2.equals("")) {//resolve case
                    for (Vertex v : newVertices.getVertices()) {
                        String newVertexClusterId = v.getPropertyValue("ClusterId").toString();
                        cluster.removeFromVertices(v.getId());
                        if (newVertexClusterId.equals(clusterClusterId)) {
                            cluster.addToVertices(v);
                        }
                    }
                }
                else {//merge case
                    for (Vertex v : cluster.getVertices()) {
                        v.setProperty("ClusterId", newClusterId);
                    }
                }
                return cluster;
            }
        }
    }


    private class updateClusters implements GroupReduceFunction<Tuple2<Cluster, String>, Cluster> {
        @Override
        public void reduce(Iterable<Tuple2<Cluster, String>> input, Collector<Cluster> output) throws Exception {

            String clusterId = "";
            Collection<Cluster> clusters = new ArrayList<>();
            for (Tuple2<Cluster, String> in:input){
                clusterId = in.f1;
                clusters.add(in.f0);
            }
            Cluster cluster = update(clusters, clusterId);

            output.collect(cluster);

        }
        public Cluster update(Collection<Cluster> clusters, String clusterId){
            Collection<Vertex> vertices = new ArrayList<>();
            Collection<Edge> links = new ArrayList<>();
            Collection<Edge> intraLinks = new ArrayList<>();
            Collection<Edge> interLinks = new ArrayList<>();
            Collection<Tuple3<Edge, Vertex, Vertex>> vertexPairs = new ArrayList<>();

            for (Cluster cluster:clusters){
                for (Vertex vertex: cluster.getVertices()){
                    if (!vertices.contains(vertex))
                        vertices.add(vertex);
                }
                for (Edge link: cluster.getInterLinks()){
                    if (!links.contains(link))
                        links.add(link);
                }
                for (Edge link: cluster.getIntraLinks()){
                    if (!links.contains(link))
                        links.add(link);
                }
            }

            for (Edge link: links){
                Vertex f0 = null;
                Vertex f1 = null;
                for (Vertex v: vertices){
                    if (v.getId().equals(link.getSourceId()))
                        f0 = v;
                    else if (v.getId().equals(link.getTargetId()))
                        f1 = v;
                }
                vertexPairs.add(Tuple3.of(link, f0, f1));
            }
            for (Tuple3<Edge, Vertex, Vertex> pair: vertexPairs) {
                if(pair.f1 == null && pair.f2==null);
                else if(pair.f1 == null || pair.f2==null)
                    interLinks.add(pair.f0);
                else {
//                    String[] f1ClusterIds = pair.f1.getPropertyValue("ClusterId").toString().split(",");
//                    String[] f2ClusterIds = pair.f2.getPropertyValue("ClusterId").toString().split(",");
//                    if (hasIntersection(f1ClusterIds, f2ClusterIds))
//                        interLinks.add(pair.f0);
//                    else
//                        intraLinks.add(pair.f0);
                    List<String> srcClusterId = Arrays.asList(pair.f1.getPropertyValue("ClusterId").toString().split(","));
                    List<String> trgtClusterId = Arrays.asList(pair.f2.getPropertyValue("ClusterId").toString().split(","));

                    if(srcClusterId.contains(clusterId) && trgtClusterId.contains(clusterId)) {
                        intraLinks.add(pair.f0);
                        if (srcClusterId.size()>1 || trgtClusterId.size()>1)
                            interLinks.add(pair.f0);
                    }
                    else if((srcClusterId.contains(clusterId) && !trgtClusterId.contains(clusterId)) || (!srcClusterId.contains(clusterId) && trgtClusterId.contains(clusterId)))
                        interLinks.add(pair.f0);

                }
            }
            return new Cluster(vertices, intraLinks, interLinks, clusterId, "");
        }
    }

    private class integrateVertices implements GroupReduceFunction<Tuple3<Vertex, String, String>, Tuple3<Cluster, String, String>> {
        @Override
        public void reduce(Iterable<Tuple3<Vertex, String, String>> iterable, Collector<Tuple3<Cluster, String, String>> collector) throws Exception {
                Collection<Vertex> vertices = new ArrayList<>();
                String oldClsId = "";
                String newClsId = "";

            for (Tuple3<Vertex, String, String> i : iterable) {
                    oldClsId = i.f1;
                    if (i.f0 != null) { // resolve
                        i.f0.setProperty("ClusterId", i.f2);
                        vertices.add(i.f0);
                    }
                    else {//merge case
                        newClsId = i.f2;
                    }
            }
            collector.collect(Tuple3.of(new Cluster(vertices), oldClsId, newClsId));
        }
    }
}
































