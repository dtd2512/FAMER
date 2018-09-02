package org.gradoop.famer.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.MergeCenter;
import org.gradoop.famer.clustering.serialClustering.SerialMergeCenter;

public class SerialTest  {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String inputDir = args[0] ; // example: "/home/alieh/Documents/Datasets/DBLPGS";
        String degree= "80";
        double serialPr, serialRe, serialFM, Pr, Re, FM;
        Pr= Re = FM = 0;
        serialPr = serialRe = serialFM = 0;
        JSONDataSource dataSource =
                new JSONDataSource (inputDir + "/Json/serialgraphHeads"+degree+".json", inputDir + "/Json/serialvertices"+degree+".json", inputDir + "/Json/serialedges"+degree+".json", config);
        LogicalGraph input = dataSource.getLogicalGraph();

        LogicalGraph CenterResult = input.callForGraph(new MergeCenter(0,0.85,false, clustering.ClusteringOutputType.Graph));

//        ComputeClusteringQualityMeasures ccqm0 = new ComputeClusteringQualityMeasures(System.currentTimeMillis(),inputDir+"/perfectMapping.csv",",",CenterResult,"pubId",false);
//        Pr = ccqm0.computePrecision();
//        Re = ccqm0.computeRecall();
//        FM = ccqm0.computeFM();
        SerialMergeCenter smc = new SerialMergeCenter();

        if (smc.doSerialClustering(0,0.85,inputDir+"/test/vertices"+degree+".txt",inputDir+"/test/edges"+degree+".txt",inputDir+"/test/res"+degree+".txt")) {

            DataSet<String> lines = env.readTextFile(inputDir + "/test/res"+degree+".txt");
            DataSet<Tuple2<String, String>> vertexIdClusterId = lines.map(new MapFunction<String, Tuple2<String, String>>() {
                public Tuple2<String, String> map(String in) {
                    String[] inArray = in.split(",");
                    String cids ="";
                    for (int i=1;i<inArray.length;i++)
                        cids += ","+ inArray[i];
                    return Tuple2.of(inArray[0], cids.substring(1,cids.length()));
                }
            });

            DataSet<Tuple2<Vertex, String>> vertexGradoopId = input.getVertices().map(new MapFunction<Vertex, Tuple2<Vertex, String>>() {
                public Tuple2<Vertex, String> map(Vertex in) {
                    return Tuple2.of(in, in.getId().toString());
                }
            });
            DataSet<Vertex> clusteredVertices =
                    vertexIdClusterId.join(vertexGradoopId).where(0).equalTo(1).with(new JoinFunction<Tuple2<String, String>, Tuple2<Vertex, String>, Vertex>() {
                        public Vertex join(Tuple2<String, String> in1, Tuple2<Vertex, String> in2) {
                            in2.f0.setProperty("ClusterId", in1.f1);
                            return in2.f0;
                        }
                    });

            LogicalGraph Result = config.getLogicalGraphFactory().fromDataSets(input.getGraphHead(), clusteredVertices, input.getEdges());


///*****Compute Clustering Measures for Correlation Results*********************/
///****************************************************************************/
///***************************************************************************/
//            ComputeClusteringQualityMeasures ccqm = new ComputeClusteringQualityMeasures(inputDir+"/perfectMapping.csv",",",Result,"pubId",false);
//            serialPr = ccqm.computePrecision();
//            serialRe = ccqm.computeRecall();
//            serialFM = ccqm.computeFM();




//            DataSet<Tuple3<String,String,String>> idc1c2 = vertexIdClusterIdCenter.join(vertexIdClusterId).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple4<String, String, String, String>>() {
//                public Tuple4<String, String, String, String> join(Tuple2<String, String> v1, Tuple2<String, String> v2) {
//                        return Tuple4.of(v1.f0,v1.f1,v2.f0,v2.f1);
//                }
//            }).flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Tuple3<String, String, String>>() {
//                public void flatMap(Tuple4<String, String, String, String> in, Collector<Tuple3<String, String, String>> out) {
//                    if(!in.f1.equals(in.f3))
//                        out.collect(Tuple3.of(in.f0,in.f1,in.f3));
//                }
//            });
//            DataSet<Tuple2<String, String>> idprio = input.getVertices().map(new MapFunction<VertexPojo, Tuple2<String, String>>() {
//                public Tuple2<String, String> map(VertexPojo in) {
//                    return Tuple2.of(in.getId().toString(),in.getPropertyValue("VertexPriority").toString());
//                }
//            });
//            idc1c2.join(idprio).where(0).equalTo(0).with(new JoinFunction<Tuple3<String,String,String>, Tuple2<String, String>, Tuple3<String,String,String>>() {
//                public Tuple3<String,String,String> join(Tuple3<String,String,String> v1, Tuple2<String, String> v2) {
//                    return Tuple3.of(v2.f1,v1.f1,v1.f2);
//                }
//            }).print();

        }
//        System.out.println("***************************");
//        CenterResult.getVertices().flatMap(new FlatMapFunction<VertexPojo, String>() {
//            public void flatMap(VertexPojo in, Collector<String> out) {
//                if(in.getPropertyValue("IsCenter").getBoolean())
//                    out.collect(in.getPropertyValue("VertexPriority").toString());
//            }
//        }).print();
        System.out.println("Pr: "+Pr+" serialPr: "+serialPr);
        System.out.println("Re: "+Re+" serialRe: "+serialRe);
        System.out.println("FM: "+FM+" serialFM: "+serialFM);
        System.out.println("*******************************");
        System.out.println("Pr: "+(Pr-serialPr));
        System.out.println("Re: "+(Re-serialRe));
        System.out.println("FM: "+(FM-serialFM));



    }

}
