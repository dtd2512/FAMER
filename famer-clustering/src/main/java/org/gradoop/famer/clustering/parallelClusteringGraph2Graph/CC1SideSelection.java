package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;

/**
 * The implementation of limited Correlation Clustering (Pivot). Centers are selected from only the specified data source (type)
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clustering.clustering;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ModifyGraphforClustering;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.LongMaxAggregator;


public class CC1SideSelection
        implements UnaryGraphToGraphOperator {
    private static String centerType;
    private static boolean IsEdgesFull;
    private static clustering.ClusteringOutputType  clusteringOutputType;

    public CC1SideSelection(String centerSideType, boolean IsEdgesFullIn, clustering.ClusteringOutputType ClusteringOutputType){
        centerType = centerSideType;
        IsEdgesFull = IsEdgesFullIn;
        clusteringOutputType = ClusteringOutputType;
    }

    public String getName() {
        return CC1SideSelection.class.getName();
    }

    public LogicalGraph execute(LogicalGraph input) {// set up execution environment
        input = input.callForGraph(new ModifyGraphforClustering());

        DataSet<Tuple1<GradoopId>> centerSideGradoopIds = input.getVertices().flatMap(new FlatMapFunction<Vertex, Tuple1<GradoopId>>() {
            public void flatMap(Vertex in, Collector<Tuple1<GradoopId>> out) {
                if (in.getPropertyValue("type").toString().equals(centerType)) {
                    out.collect(Tuple1.of(in.getId()));
                }
            }
        });
        DataSet<Tuple2<Edge,GradoopId>> edgesAndSrcIds =
                input.getEdges().map(new MapFunction<Edge, Tuple2<Edge,GradoopId>>() {
                    public Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId> map(org.gradoop.common.model.impl.pojo.Edge in) {
                        return Tuple2.of(in, in.getSourceId());
                    }
                });
        DataSet<Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId>> edgesAndTargetIds =
                input.getEdges().map(new MapFunction<org.gradoop.common.model.impl.pojo.Edge, Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId>>() {
                    public Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId> map(org.gradoop.common.model.impl.pojo.Edge in) {
                        return Tuple2.of(in, in.getTargetId());
                    }
                });
        DataSet<org.gradoop.common.model.impl.pojo.Edge> inputEdges1 = edgesAndSrcIds.join(centerSideGradoopIds).where(1).equalTo(0)
                .with(new JoinFunction<Tuple2<Edge,GradoopId>, Tuple1<GradoopId>, Edge>() {
                    public org.gradoop.common.model.impl.pojo.Edge join(Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId> in1, Tuple1<GradoopId> in2) {
                        return in1.f0;
                    }
                });
        DataSet<org.gradoop.common.model.impl.pojo.Edge> inputEdges2 = edgesAndTargetIds.join(centerSideGradoopIds).where(1).equalTo(0)
                .with(new JoinFunction<Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId>, Tuple1<GradoopId>, org.gradoop.common.model.impl.pojo.Edge>() {
                    public org.gradoop.common.model.impl.pojo.Edge join(Tuple2<org.gradoop.common.model.impl.pojo.Edge,GradoopId> in1, Tuple1<GradoopId> in2) {
                        GradoopId srcId = in1.f0.getSourceId();
                        GradoopId trgtId = in1.f0.getTargetId();
                        in1.f0.setSourceId(trgtId);
                        in1.f0.setTargetId(srcId);
                        return in1.f0;
                    }
                });
        DataSet<org.gradoop.common.model.impl.pojo.Edge> inputEdges = null;
        if (IsEdgesFull)
            inputEdges = inputEdges1;
        else
            inputEdges = inputEdges1.union(inputEdges2);

        Graph graph = Graph.fromDataSet(
               input.getVertices().map(new ToGellyVertexWithIdValue()),
                inputEdges.flatMap(new ToGellyEdgeforSGInput()),
                input.getConfig().getExecutionEnvironment()
        );
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        LongMaxAggregator longMaxAggregator = new LongMaxAggregator();
        parameters.registerAggregator("MaxDegree", longMaxAggregator);
        parameters.setSolutionSetUnmanagedMemory(true);
        int maxIterations = Integer.MAX_VALUE;

        Graph result = graph.runScatterGatherIteration(
                new myScatter(), new myGather(), maxIterations, parameters);

        LogicalGraph resultLG = input.getConfig().getLogicalGraphFactory().fromDataSets(result.getVertices().map(new ToGradoopVertex()));
//        LogicalGraph resultLG = LogicalGraph.fromDataSets(result.getVertices().map(new ToGradoopVertex()), input.getEdges(), input.getConfig());
        if (clusteringOutputType.equals(clustering.ClusteringOutputType.GraphCollection))
            resultLG = resultLG.callForGraph(new RemoveInterClustersLinks());
        return resultLG;
    }





    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
            GradoopId id = in.getId();
//            in.setProperty("VertexPriority", Long.parseLong(in.getPropertyValue("id").toString()));
//            Long vp = Long.parseLong(in.getPropertyValue("VertexPriority").toString());
//            in.f1.setProperty("VertexPriority",in.f0);
            in.setProperty("ClusterId", 0L);
            in.setProperty("degree", 0);
            in.setProperty("IsCenter", false);
            in.setProperty("roundNo", 0);
            if (in.getPropertyValue("type").toString().equals(centerType))
                in.setProperty("centerSide",true);
            else
                in.setProperty("centerSide",false);
            return new org.apache.flink.graph.Vertex(id,in);
        }
    }
    public static final class ToGradoopVertex
            implements MapFunction<org.apache.flink.graph.Vertex, Vertex> {
        public Vertex map(org.apache.flink.graph.Vertex in) throws Exception {
            return (Vertex) in.getValue();
        }
    }

    public class ToGellyEdgeforSGInput<E extends EPGMEdge>
            implements FlatMapFunction<E, org.apache.flink.graph.Edge<GradoopId, Double>> {
        public void flatMap(E e, Collector<org.apache.flink.graph.Edge<GradoopId, Double>> out) throws Exception {

            out.collect(new org.apache.flink.graph.Edge(e.getSourceId(), e.getTargetId(), 0.0));
        }
    }

/**************************************************************************************************************************/
/**************************************************************************************************************************/
/**************************************************************************************************************************/
    /**************************************************************************************************************************/


    public static final class myScatter extends ScatterFunction<GradoopId, Vertex, Long, Double> {

        public void sendMessages(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex) {
            // aggregate the partial value
            int SubSuperStep = getSuperstepNumber() % 3;

            switch (SubSuperStep){
                case 1 : // find max degree

                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0) {
                        for (org.apache.flink.graph.Edge<GradoopId, Double> edge : getEdges()) {
                            sendMessageTo(edge.getTarget(), 1L);
                        }
                        sendMessageTo(vertex.getId(),0L);
                    }
                    break;

                case 2: //select centers

                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0){
                        if(vertex.f1.getPropertyValue("centerSide").getBoolean()) {
                            double randomNum = Math.random();
                            LongValue currentMaxDegree = getPreviousIterationAggregate("MaxDegree");
                            double epsilon = 0.9;
                            double CenterSelectionProbability = epsilon / (double) (currentMaxDegree.getValue());
                            if (randomNum <= CenterSelectionProbability) {

                                for (org.apache.flink.graph.Edge<GradoopId, Double> edge : getEdges()) {
                                    sendMessageTo(edge.getTarget(), Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()));
                                }

                                sendMessageTo(vertex.getId(), Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()));


                            } else {// send fake msg
//
                                sendMessageTo(vertex.getId(), 0L);
                            }
                        }
                        else {
                            if(vertex.f1.getPropertyValue("degree").getInt() ==0) {
                                sendMessageTo(vertex.getId(), Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()));
                            }
                            else
                                sendMessageTo(vertex.getId(), 0L);
                        }
                    }
                    break;
                case 0: // grow cluster around centers

                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0){
                        if(vertex.f1.getPropertyValue("IsCenter").getBoolean()) {
                            for (org.apache.flink.graph.Edge<GradoopId, Double> edge : getEdges()) {
                                sendMessageTo(edge.getTarget(), Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()));
//
                            }
                            sendMessageTo(vertex.getId(), Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()));
                        }
                        else {//
                            sendMessageTo(vertex.getId(), 0L);
                        }
                    }
            }
        }
    }

    /**************************************************************************************************************************/

    public static final  class myGather extends GatherFunction<GradoopId, Vertex, Long> {
        LongMaxAggregator aggregator = new LongMaxAggregator();
        public void preSuperstep() {
            // retrieve the Aggregator
            aggregator = getIterationAggregator("MaxDegree");
        }
        public void updateVertex(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex, MessageIterator<Long> inMessages) {
//            System.out.println(getSuperstepNumber());

            int SubSuperStep = getSuperstepNumber() % 3;
            switch (SubSuperStep){
                case 1 : // find max degree
                    Long degree = 0L;
                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0) {
                        for (Long msg : inMessages) {

                            degree +=msg.longValue();
                        }
//                        System.out.println(degree);
                        vertex.f1.setProperty("degree",degree.intValue());
                        aggregator.aggregate(degree);
                    }
                    setNewVertexValue(vertex.f1);
                    break;
                case 2: // select centers
                    if(vertex.f1.getPropertyValue("VertexPriority")!=null) {
                        int msgCnt = 0;
                        boolean sameVertex = false;
                        Long vertexPriority = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                        for (Long msg : inMessages) {
                            if (msg != 0) {
                                if (msg.equals(vertexPriority)) { //msg != 0: do not consider fake msges
                                    sameVertex = true;
                                } else
                                    msgCnt++;
                            }
                        }

                        if (msgCnt == 0 && sameVertex) {
//                            System.out.println(vertex.f1.getPropertyValue("type"));
                            vertex.f1.setProperty("IsCenter", true);
                        }//
                    }
                    setNewVertexValue(vertex.f1);
                    break;

                case 0: // grow cluster around centers
                    vertex.f1.setProperty("roundNo",getSuperstepNumber()/3);
                    if (Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0) {// if the vertex is unassigned

                        Long MinClusterId = Long.MAX_VALUE;
                        for (Long msg : inMessages) {
                            if (msg < MinClusterId && msg!=0)
                                MinClusterId = msg;// concurrency rule no. 2
                        }

                        if (MinClusterId != Long.MAX_VALUE) {
                            vertex.f1.setProperty("ClusterId", MinClusterId);
                        }

                        setNewVertexValue(vertex.f1);
                    }
            }
        }
    }

}





