package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve;

/**
 * The implementation of Center algorithm.
 */


import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.Msg;
import org.gradoop.famer.clustering.clustering;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ModifyGraphforClustering;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;


public class ParallelResolveMsgPassing3
        implements UnaryGraphToGraphOperator {
    private int prio;
    private static clustering.ClusteringOutputType  clusteringOutputType;


    public String getName() {
        return ParallelResolveMsgPassing3.class.getName();
    }
    public ParallelResolveMsgPassing3(int prioIn,  clustering.ClusteringOutputType ClusteringOutputType){
        prio = prioIn;
        clusteringOutputType = ClusteringOutputType;
    }

    public LogicalGraph execute(LogicalGraph input) {// set up execution environment
        input = input.callForGraph(new ModifyGraphforClustering());
        Graph graph = Graph.fromDataSet(
                input.getVertices().map(new ToGellyVertexWithIdValue()),
                input.getEdges().flatMap(new ToGellyEdgeforSGInput()),
                input.getConfig().getExecutionEnvironment()
        );
        DataSet<org.gradoop.common.model.impl.pojo.Edge> edges  = input.getEdges();
        GradoopFlinkConfig config = input.getConfig();
        input = null;


        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        LongSumAggregator phase = new LongSumAggregator();
        parameters.registerAggregator("Phase", phase);


        parameters.setSolutionSetUnmanagedMemory(true);


        Graph result = graph.runScatterGatherIteration(new myScatter(), new myGather(), Integer.MAX_VALUE, parameters);

        LogicalGraph resultLG = config.getLogicalGraphFactory().fromDataSets(result.getVertices().map(new ToGradoopVertex()), edges);
//        LogicalGraph resultLG = LogicalGraph.fromDataSets(result.getVertices().map(new ToGradoopVertex()), input.getEdges(),input.getConfig());
        if (clusteringOutputType.equals(clustering.ClusteringOutputType.GraphCollection))
            resultLG = resultLG.callForGraph(new RemoveInterClustersLinks());

        return resultLG;
    }

    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
            GradoopId id = in.getId();
            in.setProperty("ClusterId", in.getPropertyValue("VertexPriority").toString());
            in.setProperty("srces", in.getPropertyValue("type").toString());
            return new org.apache.flink.graph.Vertex<GradoopId, Vertex>(id, in);
        }
    }
    public static final class ToGradoopVertex
            implements MapFunction<org.apache.flink.graph.Vertex, Vertex> {
        public Vertex map(org.apache.flink.graph.Vertex in) throws Exception {
            return (org.gradoop.common.model.impl.pojo.Vertex) in.getValue();
        }
    }

    public class ToGellyEdgeforSGInput<E extends EPGMEdge>
            implements FlatMapFunction<E, Edge<GradoopId, Integer>> {
        public void flatMap(E e, Collector<Edge<GradoopId, Integer>> out) throws Exception {
            out.collect(new Edge(e.getSourceId(), e.getTargetId(), Integer
                    .parseInt(e.getPropertyValue("prio").toString())));
//            if (!IsEdgesFull)

            out.collect(new Edge(e.getTargetId(), e.getSourceId(), Integer.parseInt(e.getPropertyValue("prio").toString())));
        }
    }

/**************************************************************************************************************************/
/**************************************************************************************************************************/
/**************************************************************************************************************************/
    /**************************************************************************************************************************/


    public static final class myScatter extends ScatterFunction<GradoopId, Vertex, Msg, Integer> {


        LongSumAggregator phase = new LongSumAggregator();
        Long phaseNo;

        public void preSuperstep() {
            // retrieve the Aggregator
            if (getSuperstepNumber()==1)
                phaseNo = 1l;
            else {
                System.out.println(getIterationAggregator("Phase").toString()+" "+"aaaaaaaaaaaaaaaaaaaaaaaaaaa");
                phaseNo = Long.parseLong(getIterationAggregator("Phase").toString());

            }


        }
        public myScatter(){
//
//            if (true)
//                phase.aggregate(1);
//            else
//                phase.aggregate(0);

        }

        public void sendMessages(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex) {
//            phaseNo = Long.parseLong(getIterationAggregator("Phase").toString());



            if(phaseNo == 1){// find max



                // find the max edge
                int maxPrio = -1;
                Edge<GradoopId, Integer>  maxEdge = null;
//                if (vertex.getId().toString().equals("58a18b2b9184902189edec94")){
//                    int cnt=0;
//                    for (Edge<GradoopId, Integer> edge : getEdges()) {cnt++;}
//                    System.out.print("000000000000000000000000000000000000 "+cnt);
//                }
                for (Edge<GradoopId, Integer> edge : getEdges()) {
                    if (edge.getValue().intValue() > maxPrio){
                        maxEdge = edge;
                        maxPrio = edge.getValue().intValue();
                    }
                }

                if (maxEdge != null) {
//                    if (maxEdge.getTarget().compareTo(vertex.getId()) < 0){
                        Msg msg = new Msg(vertex.getId().toString(), maxPrio);
                        sendMessageTo(maxEdge.getTarget(), msg);
                        sendMessageTo(vertex.getId(), msg);
//                    if (maxEdge.getSource().equals(vertex.getId()))

                    System.out.println("fffffffffffffffff " + maxPrio + " "+vertex.getId());
//                        System.out.println("fffffffffff  "+maxPrio + " v: "+vertex.getId()+" s: "+maxEdge.getSource()+" t: "+maxEdge.getTarget()+" edgeVal: "+maxEdge.getValue().intValue());


//                    }
                }
                phase.aggregate(1);


//                String sources = vertex.f1.getPropertyValue("srces").toString();
//                String clusterId = vertex.f1.getPropertyValue("ClusterId").toString();
//
//                Edge<GradoopId, Integer> maxEdge = new Edge(null, null, 0d);
//                for (Edge<GradoopId, Integer> edge : getEdges()) {
//                    if (edge.getValue().intValue() > maxEdge.getValue().intValue()){
//                        maxEdge = edge;
//                    }
//                }
//                if (maxEdge.getTarget() != null) {
//                    Msg msg = new Msg(sources, maxEdge.getValue(), maxEdge.getSource(), clusterId);
//                    sendMessageTo(maxEdge.getTarget(), msg);
//                }
//                Msg msg = new Msg(null, -1, null, null); // fake msg
//                sendMessageTo(vertex.getId(), msg);

            }
            else {
                System.out.println("kkkkkkkkkkkkkkkkk");

                phase.aggregate(0);
            }
//            else if(phaseNo == 2) {// exchange clusterIds
//            }
//
//            else if(phaseNo == 3) {// exchange clusterIds
//            }

        }
    }

    /**************************************************************************************************************************/

    public static final  class myGather extends GatherFunction<GradoopId, Vertex, Msg> {
        LongSumAggregator phase = new LongSumAggregator();
        Long phaseNo;
        public void preSuperstep() {
            // retrieve the Aggregator
            if (getSuperstepNumber()==1)
                phaseNo = 1l;
            else
                phaseNo = Long.parseLong(("Phase").toString());
        }

        public myGather() {
//            if (getSuperstepNumber()==1)
//                phase.aggregate(1);
//            else
//                phase.aggregate(0);

        }
        public void updateVertex(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex, MessageIterator<Msg> inMessages) {
//            phaseNo = Long.parseLong(getIterationAggregator("Phase").toString());
            System.out.println(phaseNo+" llll");

            if (phaseNo == 1) {

                Msg max = new Msg();
                Integer myBestPriority = -1;
                for (Msg msg : inMessages) { // TODO: The possibility of receiving same priorities
//                    if (!msg.isFake()){
                        if (msg.getContent().equals(vertex.getId().toString())){
                            myBestPriority = msg.getPriority();
                        }
                        else if (msg.getPriority()> max.getPriority()){
                            max = msg;
                        }
//                    }
                }
                if (max.getPriority()!= -1 && max.getPriority() == myBestPriority) {
                    System.out.println("ggggggggggggggggggggg");
                }
                else {
                    System.out.println("xxxxxxxxxxxxx "+vertex.getId()+" "+max.getPriority()+" "+ myBestPriority);
                }

                if (max.getPriority()!= -1 && max.getPriority() == myBestPriority){

                    vertex.f1.setProperty("desId",max.getContent());
                    setNewVertexValue(vertex.f1);
                }
                phase.aggregate(0);
            }
            else {}

        }
    }


}





