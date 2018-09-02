package org.gradoop.famer.common.Quality.ClusteredGraph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.Quality.ClusteredGraph.functions.*;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.count.Count;

/**
 * Computes Precision, Recall, and FMeasue of the input
 * The input can be a GraphCollection or a LogicalGraph.
 * It is implemented using both groupby and join. The codes related to the join are now commented.
 * In the case of using "groupby", cluster size is computed as well.
 */
public class ComputeClusteringQualityMeasures {
    private static String GTPath;
    private static String GTSplitter;
    private GraphCollection inputGrphCltion;
    private LogicalGraph inputGrph;
    private Double aveClusterSize;


    Long minClusterSize;
    private Long maxClusterSize;
    private Long clusterSize;
    private Integer srcNo;


    private static DataSet<Tuple2<String, Long>> tpset;
    private static DataSet<Tuple2<String, Long>> apset;
    private static DataSet<Tuple2<String, Long>> gtRecorsNoSet;
    private static DataSet<Tuple2<String, Long>> clusterSizeSet;
    private static DataSet<Tuple2<String, Long>> clusterNoSet;
    private static DataSet<Tuple2<String, Long>> minClusterSizeSet;
    private static DataSet<Tuple2<String, Long>> maxClusterSizeSet;
    private static DataSet<Tuple2<String, Long>> perfectClusterNoSet;
    private static DataSet<Tuple2<String, Long>> singletonSet;
    private static DataSet<Tuple2<String, Long>> perfectCompleteClusterNoSet;





    private static DataSet<Tuple2<String, Long>> repAPSet;
    private static DataSet<Tuple2<String, Long>> repTPSet;

    private static String vertexIdLabel;
    private static boolean isGTFull;
    private static ExecutionEnvironment env;
    private static Long tp;
    private static Long ap;
    private static Long gtRecorsNo;
    private static Long clusterNo;
    private static Long repPositiveNo;
    private static Long repTruePositiveNo;

    private static boolean isWithinDataSetMatch;
    private static boolean hasOverlap;
    private Long perfectClusterNo;
    private Long perfectCompleteClusterNo;

    private Long singletons;
    public void setSrcNo(Integer SrcNo){srcNo = SrcNo;}

    private void constructor(String groundTruthFilepath, String groundTruthSplitter, String VertexIdLabel, boolean isGroundTruthFull, boolean IsWithinDataSetMatch, boolean HasOverlap, Integer SourceNo){
        repAPSet = env.fromElements(Tuple2.of("repAP", 0l));
        repTPSet = env.fromElements(Tuple2.of("repTP", 0l));
        tpset = apset = gtRecorsNoSet = clusterSizeSet = null;
        ap = tp = gtRecorsNo = clusterNo = clusterSize =  minClusterSize = maxClusterSize= perfectClusterNo = singletons = perfectCompleteClusterNo = -1L;
        aveClusterSize = -1d;
        GTPath = groundTruthFilepath.trim();
        GTSplitter = groundTruthSplitter;
        vertexIdLabel = VertexIdLabel;
        isGTFull = isGroundTruthFull;
        isWithinDataSetMatch = IsWithinDataSetMatch;
        hasOverlap = HasOverlap;
        srcNo = SourceNo;
    }

    public ComputeClusteringQualityMeasures(String groundTruthFilepath, String groundTruthSplitter, GraphCollection ClusteringResult, String VertexIdLabel, boolean isGroundTruthFull, boolean IsWithinDataSetMatch, boolean HasOverlap, Integer SourceNo) {
        inputGrphCltion = ClusteringResult;
        inputGrph = null;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
        constructor(groundTruthFilepath, groundTruthSplitter, VertexIdLabel, isGroundTruthFull, IsWithinDataSetMatch, HasOverlap, SourceNo);
    }

    public ComputeClusteringQualityMeasures(String groundTruthFilepath, String groundTruthSplitter, LogicalGraph ClusteringResult, String VertexIdLabel, boolean isGroundTruthFull, boolean IsWithinDataSetMatch, boolean HasOverlap, Integer SourceNo) {
        inputGrphCltion = null;
        inputGrph = ClusteringResult;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
        constructor(groundTruthFilepath, groundTruthSplitter, VertexIdLabel, isGroundTruthFull, IsWithinDataSetMatch, HasOverlap,SourceNo);
    }

    public void computeSets() throws Exception {


        DataSet<Tuple2<String, String>> groundTruthFile = env.readTextFile(GTPath).flatMap(new GT2Tuple2(GTSplitter));
        DataSet<Vertex> resultVertices;
        if (inputGrphCltion != null) resultVertices = inputGrphCltion.getVertices();
        else resultVertices = inputGrph.getVertices();

        DataSet<Tuple3<String, String, String>> verticesClusterIdsTypes = resultVertices.flatMap(new Vertex2VertexData("recId","ClusterId", "type"));


//        System.out.println("verticesClusterIdsTypes.count(): "+verticesClusterIdsTypes.count());
//        System.out.println("groundTruthFile.count(): "+groundTruthFile.count());
//        DataSet<Tuple1<String>> yy = verticesClusterIdsTypes.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<String, String, String>, Tuple1<String>>() {
//            @Override
//            public void reduce(Iterable<Tuple3<String, String, String>> iterable, Collector<Tuple1<String>> collector) throws Exception {
//                int cnt = 0;
//                String a="";
//                for (Tuple3<String, String, String> i:iterable){
//                    cnt++;
//                    a=i.f0;
//                }
//                if (cnt > 1)
//                    collector.collect(Tuple1.of(a));
//            }
//        });
////        yy.print();
//        System.out.println("yy.count(): "+yy.count());
        /////////////////////////////////////////////////////////////////////////////////
//        System.out.println("phase1.count(): "+verticesClusterIdsTypes.join(groundTruthFile).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
//            public Tuple2<String, String> join(Tuple3<String, String, String> first, Tuple2<String, String> second) throws Exception {
//                return Tuple2.of(first.f1, second.f1);
//            }
//        }).count());
//        DataSet<Tuple1<String>> t = groundTruthFile.
//                flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple1<String>>() {
//            @Override
//            public void flatMap(Tuple2<String, String> in, Collector<Tuple1<String>> out) throws Exception {
//                    out.collect(Tuple1.of(in.f0 + "," + in.f1));
//                    out.collect(Tuple1.of(in.f1 + "," + in.f0));
//            }
//        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple1<String>>() {
//            @Override
//            public void reduce(Iterable<Tuple1<String>> iterable, Collector<Tuple1<String>> collector) throws Exception {
//                int cnt=0;
//                for (Tuple1<String> i:iterable){
//                    cnt++;
//                }
//                if (cnt>1)
//                    collector.collect(Tuple1.of("..."));
//            }
//        });
//        System.out.println("t.count(): "+t.count());


        DataSet<Long> mytp = verticesClusterIdsTypes.join(groundTruthFile).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
            public Tuple2<String, String> join(Tuple3<String, String, String> first, Tuple2<String, String> second) throws Exception {
                return Tuple2.of(first.f1, second.f1);
            }
        }).join(verticesClusterIdsTypes).where(1).equalTo(0).with(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple2<String, String>>() {
            public Tuple2<String, String> join(Tuple2<String, String> first, Tuple3<String, String, String> second) throws Exception {
                return Tuple2.of(first.f0, second.f1);
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, String>, Long>() {
            public void flatMap(Tuple2<String, String> value, Collector<Long> out) throws Exception {
                if (value.f0.equals(value.f1))
                    out.collect(1l);
            }
        });

////////////////////////////////////////////////////////////////////////////////////
        ///////////// join instead of group by
//        DataSet<Long> myap = null;
//        if (isWithinDataSetMatch) {
//            myap = verticesClusterIdsTypes.join(verticesClusterIdsTypes).where(1).equalTo(1).with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
//                public Tuple4<String, String, String, String> join(Tuple3<String, String, String> first, Tuple3<String, String, String> second) throws Exception {
//                    return Tuple4.of(first.f0, second.f0, first.f2, second.f2);
//                }
//            }).flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Long>() {
//                public void flatMap(Tuple4<String, String, String, String> value, Collector<Long> out) throws Exception {
//                    if (!value.f0.equals(value.f1))
//                        out.collect(1l);
//                }
//            });
//        }
//        else {
//            myap = verticesClusterIdsTypes.join(verticesClusterIdsTypes).where(1).equalTo(1).with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
//                public Tuple4<String, String, String, String> join(Tuple3<String, String, String> first, Tuple3<String, String, String> second) throws Exception {
//                    return Tuple4.of(first.f0, second.f0, first.f2, second.f2);
//                }
//            }).flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Long>() {
//                public void flatMap(Tuple4<String, String, String, String> value, Collector<Long> out) throws Exception {
//                    if (!value.f0.equals(value.f1) && !value.f2.equals(value.f3))
//                        out.collect(1l);
//                }
//            });
//        }
//        apset = Count.count(myap).map(new MapFunction<Long, Tuple2<String, Long>>() {
//            public Tuple2<String, Long> map(Long a) {
//                return Tuple2.of("ap", a);
//            }
//        });

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
        ///////////// group by instead of join
        DataSet<Tuple2<Long,Long>> ap_clstrsize = null;

        ap_clstrsize = verticesClusterIdsTypes.groupBy(1).reduceGroup(new ComputeAP_Clstersize(isWithinDataSetMatch));
        apset = ap_clstrsize.reduce(new reduceTuple2toSet(0)).map(new Tuple2toSet("ap",0));
        maxClusterSizeSet = ap_clstrsize.aggregate(Aggregations.MAX, 1).map(new Tuple2toSet("maxcs",1));
        minClusterSizeSet = ap_clstrsize.aggregate(Aggregations.MIN, 1).map(new Tuple2toSet("mincs",1));
        clusterNoSet = Count.count(ap_clstrsize).map(new LongtoSet("cn"));
        clusterSizeSet = ap_clstrsize.reduce(new reduceTuple2toSet(1)).map(new Tuple2toSet("cs",1));


        perfectClusterNoSet = verticesClusterIdsTypes.groupBy(1).reduceGroup(new perfectClusterNoReducer("pcn")).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1+value2.f1);
            }
        });
        perfectCompleteClusterNoSet = verticesClusterIdsTypes.groupBy(1).reduceGroup(new perfectCompleteClusterNoReducer("pccn", srcNo)).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1+value2.f1);
            }
        });
        singletonSet = verticesClusterIdsTypes.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, Long>>() {
                                                                          @Override
                                                                          public void reduce(Iterable<Tuple3<String, String, String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
                                                                             int cnt = 0;
                                                                             for (Tuple3<String, String, String> value: values)
                                                                                 cnt++;
                                                                              if (cnt==1)
                                                                                  out.collect(Tuple2.of("single",1l));
                                                                              else
                                                                                  out.collect(Tuple2.of("single",0l));

                                                                          }
                                                                      }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1+value2.f1);
            }
        });

////////////////////////////////////////////////////////////////////////////////////
                tpset = Count.count(mytp).map(new LongtoSet("tp"));
        gtRecorsNoSet = Count.count(groundTruthFile).map(new LongtoSet("gt"));
        if (hasOverlap) {
            DataSet<Tuple2<String, String>> pairedVerticesClusterId = verticesClusterIdsTypes.join(verticesClusterIdsTypes).where(1).equalTo(1).with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple6<String, String, String, String, String, String>>() {
                public Tuple6<String, String, String, String, String, String> join(Tuple3<String, String, String> in1, Tuple3<String, String, String> in2) {
                        return Tuple6.of(in1.f0, in1.f1, in1.f2, in2.f0, in2.f1, in2.f2);
//
                }
            }).flatMap(new FlatMapFunction<Tuple6<String, String, String, String, String, String>, Tuple2<String, String>>() {
                public void flatMap(Tuple6<String, String, String, String, String, String> in, Collector<Tuple2<String, String>> out) {
                    if (!in.f2.equals(in.f5) || isWithinDataSetMatch)
                            if (in.f0.compareTo(in.f3)<0)
                                out.collect(Tuple2.of(in.f0 + "," + in.f3, in.f1));
                }
            });
            repAPSet = pairedVerticesClusterId.groupBy(0).reduceGroup(new ComputeCountbyReduce()).reduce(new reduceLongtoSet()).map(new LongtoSet("repAP"));
            DataSet<Tuple1<String>> tempGroundTruth = groundTruthFile.map(new Concat());
            repTPSet = pairedVerticesClusterId.join(tempGroundTruth).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, String>, Tuple1<String>, Tuple2<String, String>>() {
                public Tuple2<String, String> join(Tuple2<String, String> first, Tuple1<String> second) throws Exception {
                    return first;
                }
            }).groupBy(0).reduceGroup(new ComputeCountbyReduce()).reduce(new reduceLongtoSet()).map(new LongtoSet("repTP"));
        }
    }

    private void computeValues() throws Exception {

        if (apset == null)
            computeSets();


        DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(gtRecorsNoSet).union(repAPSet).union(repTPSet).union(maxClusterSizeSet)
                .union(minClusterSizeSet).union(clusterNoSet).union(clusterSizeSet).union(perfectClusterNoSet).union(singletonSet).union(perfectCompleteClusterNoSet);
//        DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(gtRecorsNoSet).union(repAPSet).union(repTPSet);


        for (Tuple2<String, Long> i : sets.collect()) {

            if (i.f0.equals("ap")) {
                ap = i.f1;
//                System.out.println("ap: "+ap);
            }
            else if (i.f0.equals("tp")) {
                tp = i.f1;
//                System.out.println("tp: "+tp);
            }
            else if (i.f0.equals("gt")) {
                gtRecorsNo = i.f1;
//                System.out.println("gtRecorsNo: "+gtRecorsNo);
            }
            else if (i.f0.equals("repAP")) {
                repPositiveNo = i.f1;
//                System.out.println("repPositiveNo: "+repPositiveNo);
            }
            else if (i.f0.equals("repTP")) {
                repTruePositiveNo = i.f1;
//                System.out.println("repTruePositiveNo: "+repTruePositiveNo);
            }
            else if (i.f0.equals("maxcs")) {
                maxClusterSize = i.f1;
//                System.out.println("maxClusterSize: "+maxClusterSize);
            }
            else if (i.f0.equals("mincs")) {
                minClusterSize = i.f1;
//                System.out.println("minClusterSize: "+minClusterSize);
            }
            else if (i.f0.equals("cs")) {
                clusterSize = i.f1;
//                System.out.println("clusterSize: "+clusterSize);
            }
            else if (i.f0.equals("cn")) {
                clusterNo = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("pcn")) {
                perfectClusterNo = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("pccn")) {
                perfectCompleteClusterNo = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("single")) {
                singletons = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
        }
        if (!isGTFull) {
            repPositiveNo /= 2;
            ////used with join
//            ap /= 2;
        }
//        System.out.println(ap+"********eval**********"+repPositiveNo);
//        System.out.println(tp+"**********eval********"+repTruePositiveNo);
        ap -= repPositiveNo;
        tp -= repTruePositiveNo;
        aveClusterSize = (double)clusterSize/clusterNo;
    }



    public long getTP() throws Exception {
        if (tp == -1)
            computeValues();
        return tp;
    }


    public long getAP() throws Exception {
        if (ap == -1)
            computeValues();
        return ap;
    }

    public long getGtRecordsNo() throws Exception {
        if (gtRecorsNo == -1)
            computeValues();

        return gtRecorsNo;
    }

    public long getClusterNo() throws Exception {
        if (clusterNo == -1)
            computeValues();
        return clusterNo;
    }

    public Double computePrecision() throws Exception {
        if (tp == -1)
            computeValues();
        return (double) tp / ap;
    }

    public Double computeRecall() throws Exception {
        if (tp == -1)
            computeValues();
        return (double) tp / gtRecorsNo;
    }

    public Double computeFM() throws Exception {
        if (tp == -1)
            computeValues();
        double pr = computePrecision();
        double re = computeRecall();
        return 2 * pr * re / (pr + re);
    }
    public long getMinClsterSize() throws Exception {
        if (minClusterSize == -1)
            computeValues();
        return minClusterSize;
    }
    public long getMaxClsterSize() throws Exception {
        if (maxClusterSize == -1)
            computeValues();
        return maxClusterSize;
    }
    public long getSumClsterSize() throws Exception {
        if (clusterSize == -1)
            computeValues();
        return clusterSize;
    }
    public double getAveClsterSize() throws Exception {
        if (aveClusterSize == -1)
            computeValues();
        return aveClusterSize;
    }
    public Long getPerfectClusterNo() throws Exception {
        if (perfectClusterNo == -1)
            computeValues();
        return perfectClusterNo;
    }
    public Long getPerfectCompleteClusterNo() throws Exception {
        if (perfectCompleteClusterNo == -1)
            computeValues();
        return perfectCompleteClusterNo;
    }
    public Long getSingletons () throws Exception {
        if (singletons == -1)
            computeValues();
        return singletons;
    }
}

