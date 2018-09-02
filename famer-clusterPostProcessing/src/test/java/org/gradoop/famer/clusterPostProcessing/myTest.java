package org.gradoop.famer.clusterPostProcessing;



import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.multiEntity1SrcResolve;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util.sequentialResolve.resolveBulkIteration;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.filterOverlappedVertices;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.overlapLength;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.resolveReducer2;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions.updateOverlappedVertices;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.overlapResolve;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.overlapResolveBulkIteration;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.consolidateMergedClusterIds;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.functions.vertexVsClusterJoin;
import org.gradoop.famer.clusterPostProcessing.outputAnalyse.overlaps.Overlaps;
import org.gradoop.famer.clustering.clustering;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.*;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasuresWithSameIds;
import org.gradoop.famer.common.functions.*;
import org.gradoop.famer.common.maxDeltaLinkSelection.maxLinkSrcSelection;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;


import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

public class myTest {
    public static void main(String args[]) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
//
//        String inputDir = args[0];
//        JSONDataSource dataSource =
//                new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
//
//        LogicalGraph input = dataSource.getLogicalGraph();
//        LogicalGraph output = null;
//
//        Integer srcNo = Integer.parseInt(args[1]);
//
//        output = input.callForGraph(new multiEntity1SrcResolve(0d, 1, srcNo, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//
//        String outputpath = args[2];
//        JSONDataSink jsd = new JSONDataSink(outputpath + "graphHeads.json", outputpath + "vertices.json", outputpath + "edges.json", config);
//        jsd.write(output);
//        env.execute();
        String inputDir = args[0];
        Integer algorithm = Integer.parseInt(args[1]);
        switch (algorithm) {

            case 1:
                inputDir +=  "ConCom/";
                break;
            case 2:
                inputDir +=  "CCPiv/";
                break;
            case 3:
                inputDir +=  "Center0/";
                break;
            case 4:
                inputDir +=  "Center1/";
                break;
            case 5:
                inputDir +=  "MCenter0/";
                break;
            case 6:
                inputDir +=  "MCenter1/";
                break;
            case 7:
                inputDir +=  "Star10/";
                break;
            case 8:
                inputDir +=  "Star11/";
                break;

            case 9:
                inputDir +=  "Star20/";
                break;

            case 10:
                inputDir +=  "Star21/";
                break;

        }
        JSONDataSource dataSource =
                new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);

        LogicalGraph input = dataSource.getLogicalGraph();
        Integer srcNo = Integer.parseInt(args[2]);
        FileWriter fw = new FileWriter(args[3], true);

        for (int i=1; i <=Integer.parseInt(args[4]) ; i++) {
            LogicalGraph output = input.callForGraph(new multiEntity1SrcResolve(0d, 1, srcNo, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//            ComputeClusteringQualityMeasuresWithSameIds1Src eval = new ComputeClusteringQualityMeasuresWithSameIds1Src(output, "", false);
            System.out.println(output.getVertices().count());
            fw.write(args[5]+","+args[1]+","+env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+"\n");
            fw.flush();

//            fw.write(args[1] + "," + eval.computePrecision() + "," + eval.computeRecall() + "," + eval.computeFM() + "," +
//                    eval.getAveClsterSize() + "," + eval.getMaxClsterSize() + "," + eval.getSingletons() + "," + eval.getClusterNo() + "," +
//                    eval.getPerfectClusterNo() + "," + eval.getPerfectCompleteClusterNo() + "\n");
//            fw.flush();
        }

//    }
///******************************************************************************************/
//String inputDir = "/home/alieh/git44Prject/sources/geographicalDataSet/Test-Result/test3/Graphs/1/";
//    JSONDataSource dataSource =
//            new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
//
//    LogicalGraph input = dataSource.getLogicalGraph();
//
//        LogicalGraph output = null;
//
//        Integer srcNo = 4;
//
//        output = input.callForGraph(new multiEntity1SrcResolve(0d, 1, srcNo, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//        ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",
//                ",",output, "",false,true,false);
//        System.out.println(eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+","+
//                eval.getAveClsterSize()+","+eval.getMaxClsterSize()+","+eval.getSingletons()+","+eval.getClusterNo()+","+
//                eval.getPerfectClusterNo()+","+eval.getPerfectCompleteClusterNo()+"\n");

//        0.9815071659731854,0.9669779093600547,0.974188367557646,3.6313912009512483,4,54,841,841,687
//        0.9856845993996768,0.9722158961512184,0.9789039211190095,3.6357142857142857,4,54,840,840,689

/************************************************************************************/
//        ClusterCollection cc = new ClusterCollection(input);
//        DataSet<Cluster> clusters = cc.getClusterCollection();
//        DataSet<Tuple2<Cluster, String>> cluster_clusterId = clusters.map(new cluster2cluster_clusterId());
//        DataSet<Vertex> vertices = input.getVertices();
//        DataSet<Tuple2<Vertex, Integer>> vertex_overlapLength = vertices.flatMap(new overlapLength());
//        DataSet<Vertex> toBeResolvedVertices = vertex_overlapLength.minBy(1).join(vertex_overlapLength).where(1).equalTo(1).with(new joinGetSecond()).map(new getF0Tuple2());
//
// // resolve
//        DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
//        DataSet<Tuple2<Vertex, Cluster>> toBeResolvedVertex_cluster = toBeResolvedVertex_clusterId.
//        join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin());
//
//        DataSet<Vertex> updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
//        .groupBy(0).reduceGroup(new resolveReducer2());
//
// // update vertex set
//        DataSet<Tuple2<Vertex, String>> updatedVertex_gradoopId = updatedVertices.map(new vertex2vertex_gradoopId());
//
// /* remove repetitive vertices produced by cluster merging */
//        updatedVertex_gradoopId = updatedVertex_gradoopId.groupBy(1).reduceGroup(new consolidateMergedClusterIds());
//        DataSet<Tuple2<Vertex, String>> allVertex_gradoopId = vertices.map(new vertex2vertex_gradoopId());
//        vertices = allVertex_gradoopId.leftOuterJoin(updatedVertex_gradoopId).where(1).equalTo(1).with(new updateOverlappedVertices()).map(new getF0Tuple2());
//
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//
//         vertex_overlapLength = vertices.flatMap(new overlapLength());
//        vertex_overlapLength.minBy(1).print();
//         toBeResolvedVertices = vertex_overlapLength.minBy(1).join(vertex_overlapLength).where(1).equalTo(1).with(new joinGetSecond()).map(new getF0Tuple2());
////        // resolve
//         toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
//         toBeResolvedVertex_cluster = toBeResolvedVertex_clusterId.
//                join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin());
////
//         updatedVertices = toBeResolvedVertex_cluster.map(new vertex_T2vertexId_vertex_T())
//                .groupBy(0).reduceGroup(new resolveReducer2());
////
////        // update vertex set
//         updatedVertex_gradoopId = updatedVertices.map(new vertex2vertex_gradoopId());
////
// /* remove repetitive vertices produced by cluster merging */
//        updatedVertex_gradoopId = updatedVertex_gradoopId.groupBy(1).reduceGroup(new consolidateMergedClusterIds());
//         allVertex_gradoopId = vertices.map(new vertex2vertex_gradoopId());
//        vertices = allVertex_gradoopId.leftOuterJoin(updatedVertex_gradoopId).where(1).equalTo(1).with(new updateOverlappedVertices()).map(new getF0Tuple2());
//        vertices.print();


/*****************************************************************************/
//        String inputDir = "/home/alieh/temp/parallel2/";
//                String inputDir = "/home/alieh/git44Prject/sources/geographicalDataSet/Test-Result/test3/Graphs/1/";
////
//        JSONDataSource dataSource =
//                new JSONDataSource(inputDir + "graphHeads.json", inputDir + "vertices.json", inputDir + "edges.json", config);
//        LogicalGraph input = dataSource.getLogicalGraph();
////        ComputeClusteringQualityMeasuresWithSameIds1Src eval = new ComputeClusteringQualityMeasuresWithSameIds1Src(input, "clusterId",  true);
//
//
////
//        input = input.callForGraph(new multiEntity1SrcResolve(0d, 1, 4, multiEntity1SrcResolve.multiEntity1SrcResolveType.PARALLEL));
//        input = input.callForGraph(new multiEntity1SrcResolve(0d, 2, 4, multiEntity1SrcResolve.multiEntity1SrcResolveType.PARALLEL));
//        input = input.callForGraph(new multiEntity1SrcResolve(0d, 3, 4, multiEntity1SrcResolve.multiEntity1SrcResolveType.PARALLEL));


//        input = input.callForGraph(new multiEntity1SrcResolve(0d, 1, 4, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//        input = input.callForGraph(new multiEntity1SrcResolve(0d, 2, 4, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//        input = input.callForGraph(new multiEntity1SrcResolve(0d, 3, 3, multiEntity1SrcResolve.multiEntity1SrcResolveType.SEQUENTIAL));
//        input = input.callForGraph(new parallelResolve());
//        input = input.callForGraph(new maxLinkSrcSelection(0d));
//        input = input.callForGraph(new resolveBulkIteration(2));
//        System.out.println(input.getEdges().count());
//        input.getVertices().print();


//        ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",",",input,"",false,true,false);
//        System.out.println("pre: "+eval.computePrecision()+" rec: "+eval.computeRecall()+" fm: "+eval.computeFM()+" ave: "+eval.getAveClsterSize()+", max: "+ eval.getMaxClsterSize()+", sing: "+eval.getSingletons()
//                +"all clusters: "+eval.getClusterNo()+"\n");


///*************************************************************************************************************/
//        Overlaps overlaps = new Overlaps(input);
//        long overlappedVerticesNo = overlaps.getOverlappedVerticesNo();
//        long overlappedVerticesNo2 = overlaps.getOverlappedVerticesNo(2);
//        long overlappedVerticesNo3 = overlaps.getOverlappedVerticesNo(3);
//        long overlappedVerticesNo4 = overlaps.getOverlappedVerticesNo(4);
//        long overlappedVerticesNo5 = overlaps.getOverlappedVerticesNo(5);
//        long overlappedVerticesNo6 = overlaps.getOverlappedVerticesNo(6);
//        long overlappedVerticesNo7 = overlaps.getOverlappedVerticesNo(7);
//        long overlappedVerticesNo8 = overlaps.getOverlappedVerticesNo(8);
//        long overlappedVerticesNo9 = overlaps.getOverlappedVerticesNo(9);
//        long overlappedVerticesNo10 = overlaps.getOverlappedVerticesNo(10);
//        long overlappedVerticesNo11 = overlaps.getOverlappedVerticesNo(11);
//        long overlappedVerticesNo12 = overlaps.getOverlappedVerticesNo(12);
//        long overlappedVerticesNo13 = overlaps.getOverlappedVerticesNo(13);
//        long overlappedVerticesNo14 = overlaps.getOverlappedVerticesNo(14);
//        long overlappedVerticesNo15 = overlaps.getOverlappedVerticesNo(15);
//        long overlappedVerticesNo16 = overlaps.getOverlappedVerticesNo(16);
//        long overlappedVerticesNo17 = overlaps.getOverlappedVerticesNo(17);
//        long overlappedVerticesNo18 = overlaps.getOverlappedVerticesNo(18);
//        long overlappedVerticesNo19 = overlaps.getOverlappedVerticesNo(19);
//        long overlappedVerticesNo20 = overlaps.getOverlappedVerticesNo(20);
//        long overlappedVerticesNo21 = overlaps.getOverlappedVerticesNo(21);
//        long overlappedVerticesNo22 = overlaps.getOverlappedVerticesNo(22);
//        long overlappedVerticesNo23 = overlaps.getOverlappedVerticesNo(23);
//        long overlappedVerticesNo24 = overlaps.getOverlappedVerticesNo(24);
//        long overlappedVerticesNo25 = overlaps.getOverlappedVerticesNo(25);
//        long overlappedVerticesNo26 = overlaps.getOverlappedVerticesNo(26);
//        long overlappedVerticesNo27 = overlaps.getOverlappedVerticesNo(27);
//        long overlappedVerticesNo28 = overlaps.getOverlappedVerticesNo(28);
//        long overlappedVerticesNo29 = overlaps.getOverlappedVerticesNo(29);
//        long overlappedVerticesNo30 = overlaps.getOverlappedVerticesNo(30);
//        long overlappedVerticesNo31 = overlaps.getOverlappedVerticesNo(31);
//
//        System.out.println("total: "+overlappedVerticesNo);
//        System.out.println("2: "+overlappedVerticesNo2);
//        System.out.println("3: "+overlappedVerticesNo3);
//        System.out.println("4: "+overlappedVerticesNo4);
//        System.out.println("5: "+overlappedVerticesNo5);
//        System.out.println("6: "+overlappedVerticesNo6);
//        System.out.println("7: "+overlappedVerticesNo7);
//        System.out.println("8: "+overlappedVerticesNo8);
//        System.out.println("9: "+overlappedVerticesNo9);
//        System.out.println("10: "+overlappedVerticesNo10);
//        System.out.println("11: "+overlappedVerticesNo11);
//        System.out.println("12: "+overlappedVerticesNo12);
//        System.out.println("13: "+overlappedVerticesNo13);
//        System.out.println("14: "+overlappedVerticesNo14);
//        System.out.println("15: "+overlappedVerticesNo15);
//        System.out.println("16: "+overlappedVerticesNo16);
//        System.out.println("17: "+overlappedVerticesNo17);
//        System.out.println("18: "+overlappedVerticesNo18);
//        System.out.println("19: "+overlappedVerticesNo19);
//        System.out.println("20: "+overlappedVerticesNo20);
//        System.out.println("21: "+overlappedVerticesNo21);
//        System.out.println("22: "+overlappedVerticesNo22);
//        System.out.println("23: "+overlappedVerticesNo23);
//        System.out.println("24: "+overlappedVerticesNo24);
//        System.out.println("25: "+overlappedVerticesNo25);
//        System.out.println("26: "+overlappedVerticesNo26);
//        System.out.println("27: "+overlappedVerticesNo27);
//        System.out.println("28: "+overlappedVerticesNo28);
//        System.out.println("29: "+overlappedVerticesNo29);
//        System.out.println("30: "+overlappedVerticesNo30);
//        System.out.println("31: "+overlappedVerticesNo31);
//        System.out.println("problematic clusters: "+overlaps.getOverlappedClusterIds().count());
//        ComputeClusteringQualityMeasuresWithSameIds1Src eval2 = new ComputeClusteringQualityMeasuresWithSameIds1Src(input, "clusterId",  false);
//        ComputeClusteringQualityMeasures eval2 = new ComputeClusteringQualityMeasures("/home/alieh/git44Prject/sources/geographicalDataSet/RawInputFiles/pm.csv",",",input,"",false,false,true);
//
//        System.out.println("pre: "+eval2.computePrecision()+" rec: "+eval2.computeRecall()+" fm: "+eval2.computeFM()+" ave: "+eval2.getAveClsterSize()+", max: "+ eval2.getMaxClsterSize()+", sing: "+eval2.getSingletons()
//                +"all clusters: "+eval2.getClusterNo()+"\n");

/***************************************************************************************************************/
//        ComputeClusteringQualityMeasuresWithSameIds1Src eval = new ComputeClusteringQualityMeasuresWithSameIds1Src(input, "clusterId", true);
//        System.out.println( "pre: "+ eval.computePrecision() + ", rec: " + eval.computeRecall() + ", fm: " + eval.computeFM() + "\n");


/*************************************************************************************************************/
//        DataSet<Tuple2<Vertex, Integer>> vertex_overlapLength = input.getVertices().map(new MapFunction<Vertex, Tuple2<Vertex, Integer>>() {
//            @Override
//            public Tuple2<Vertex, Integer> map(Vertex in) throws Exception {
//                int oll = in.getPropertyValue("ClusterId").toString().split(",").length;
//                return Tuple2.of(in, oll);
//            }
//        });
//        DataSet<Vertex> toBeResolvedVertices = vertex_overlapLength.maxBy(1).join(vertex_overlapLength).where(1).equalTo(1).with(new joinGetSecond()).map(new getF0Tuple2());
//
//        toBeResolvedVertices.print();
/*************************************************************************************************************/


//    }


    }

}















