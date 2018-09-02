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

/** 4 ohne while
 */
public class overlapResolveOptimized4 implements UnaryGraphToGraphOperator {
    @Override
    public String getName() {
        return "overlapResolveBulkIteration";
    }
    private Integer iteration;
    public overlapResolveOptimized4(Integer iterationNo){iteration = iterationNo;}

    @Override
    public LogicalGraph execute(LogicalGraph clusteredGraph) {

        ClusterCollection cc = new ClusterCollection(clusteredGraph);
        DataSet<Cluster> clusters = cc.getClusterCollection();
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
        DataSet<Vertex> vertices = null;
        do {

            iteration--;


        /* start: bulk iteration */
            DataSet<Tuple2<Cluster, String>> cluster_clusterId = clusters.map(new cluster2cluster_clusterId());
            vertices = clusters.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());


            // find vertices that must be resolved in this iteration
            DataSet<Tuple2<Vertex, Integer>> vertex_overlapLength = vertices.flatMap(new overlapLength());
            DataSet<Vertex> toBeResolvedVertices = vertex_overlapLength.minBy(1).join(vertex_overlapLength).where(1).equalTo(1).with(new joinGetSecond()).map(new getF0Tuple2());

            // resolve
            DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
            DataSet<Tuple2<Vertex, Cluster>> toBeResolvedVertex_cluster = toBeResolvedVertex_clusterId.
                    join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin());

            DataSet<Tuple3<Vertex, String, String>> updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
                    .groupBy(0).reduceGroup(new resolveReducer3());

            // update vertex set
            clusters = cluster_clusterId.leftOuterJoin(updatedVertices).where(1).equalTo(1).with(new updateJoin());
            clusters = clusters.map(new cluster2cluster_clusterId()).groupBy(1).reduceGroup(new updateClusters());
            vertices = clusters.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());


            DataSet<Vertex> remainingToBeResolvedVertices = vertices.flatMap(new filterOverlappedVertices());
            try {
                System.out.println("remainingToBeResolvedVertices: "+ remainingToBeResolvedVertices.count());
            } catch (Exception e) {
                e.printStackTrace();
            }

        }while (iteration >0);

        return clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(vertices);

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
                    cluster.getVertices().remove(vertex);
                    if (clusterClusterId.equals(newClusterId))// if clusterId == resolved clusterid
                    {
                        vertex.setProperty("ClusterId", newClusterId);
                        cluster.getVertices().add(vertex);
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


    private class updateClusters implements GroupReduceFunction<Tuple2<Cluster, String>, Cluster> {
        @Override
        public void reduce(Iterable<Tuple2<Cluster, String>> input, Collector<Cluster> output) throws Exception {
            Collection<Vertex> vertices = new ArrayList<>();
            Collection<Edge> links = new ArrayList<>();
            Collection<Edge> intraLinks = new ArrayList<>();
            Collection<Edge> interLinks = new ArrayList<>();
            String clusterId = "";

            Collection<Tuple3<Edge, Vertex, Vertex>> vertexPairs = new ArrayList<>();

            for (Tuple2<Cluster, String> in:input){
                for (Vertex vertex: in.f0.getVertices()){
                    if (!vertices.contains(vertex))
                        vertices.add(vertex);
                }
                for (Edge link: in.f0.getInterLinks()){
                    if (!links.contains(link))
                        links.add(link);
                }
                for (Edge link: in.f0.getIntraLinks()){
                    if (!links.contains(link))
                        links.add(link);
                }
                clusterId = in.f1;
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
                if(pair.f1 == null || pair.f2==null)
                    intraLinks.add(pair.f0);
                else {
//                    String[] f1ClusterIds = pair.f1.getPropertyValue("ClusterId").toString().split(",");
//                    String[] f2ClusterIds = pair.f2.getPropertyValue("ClusterId").toString().split(",");
//                    if (hasIntersection(f1ClusterIds, f2ClusterIds))
//                        interLinks.add(pair.f0);
//                    else
//                        intraLinks.add(pair.f0);
                    List<String> srcClusterId = Arrays.asList(pair.f1.getPropertyValue("ClusterId").toString().split(","));
                    List<String> trgtClusterId = Arrays.asList(pair.f2.getPropertyValue("ClusterId").toString().split(","));
                    if (srcClusterId.size() == 1 && trgtClusterId.size()==1 && srcClusterId.get(0).equals(trgtClusterId.get(0))){ // inter
                        interLinks.add(pair.f0);
                    }
                    List<String> union = findUnion(srcClusterId, trgtClusterId);
                    for (String id:union){
                        if ((srcClusterId.contains(id) && !trgtClusterId.contains(id)) || (!srcClusterId.contains(id) && trgtClusterId.contains(id))) // intra link
                            intraLinks.add(pair.f0);
                        else if (srcClusterId.contains(id) && trgtClusterId.contains(id)) {// inter and intra link
                            intraLinks.add(pair.f0);
                            interLinks.add(pair.f0);
                        }
                    }
                }
            }
            output.collect(new Cluster(vertices, intraLinks, interLinks, clusterId, ""));

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
        private List<String> findUnion (List<String> first, List<String> second){
            List<String> outList = new ArrayList<>();
            for (String s:first){
                if (!outList.contains(s))
                    outList.add(s);
            }
            for (String s:second){
                if (!outList.contains(s))
                    outList.add(s);
            }

            return outList;
        }
    }
}
































