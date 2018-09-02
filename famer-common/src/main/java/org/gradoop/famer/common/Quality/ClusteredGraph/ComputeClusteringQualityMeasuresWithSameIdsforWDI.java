package org.gradoop.famer.common.Quality.ClusteredGraph;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/** It is used when there is no Golden Truth file and noly vertices with the same ids are true positives
 * Computes Precision, Recall, and FMeasue of the input
 * The input can be a GraphCollection or a LogicalGraph.
 *
 */
public class ComputeClusteringQualityMeasuresWithSameIdsforWDI {
    private static String GTPath;
    private static String GTSplitter;
    private GraphCollection inputGrphCltion;
    private LogicalGraph inputGrph;
    private static DataSet<Tuple2<String, Long>> tpset;
    private static DataSet<Tuple2<String, Long>> apset;
    private static DataSet<Tuple2<String, Long>> gtRecorsNoSet;
    private static DataSet<Tuple2<String, Long>> clusterNoSet;
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



    public ComputeClusteringQualityMeasuresWithSameIdsforWDI(GraphCollection ClusteringResult, String VertexIdLabel, boolean HasOverlap) {
        inputGrphCltion = ClusteringResult;
        vertexIdLabel = VertexIdLabel;
        tpset = apset = gtRecorsNoSet = clusterNoSet = null;
        ap = tp = gtRecorsNo = clusterNo = -1L;
        inputGrph = null;
        hasOverlap = HasOverlap;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
    }


    public ComputeClusteringQualityMeasuresWithSameIdsforWDI(LogicalGraph ClusteringResult, String VertexIdLabel, boolean HasOverlap) {

        inputGrphCltion = null;
        vertexIdLabel = VertexIdLabel;
        tpset = apset = gtRecorsNoSet = clusterNoSet = null;
        ap = tp = gtRecorsNo = clusterNo = -1L;
        inputGrph = ClusteringResult;
        hasOverlap = HasOverlap;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
    }

    public void computeSets() throws Exception {

        DataSet<Vertex> resultVertices = null;
        if (inputGrphCltion != null) {
            resultVertices = inputGrphCltion.getVertices();


        } else {
            resultVertices = inputGrph.getVertices();

        }

        DataSet<Tuple2<GradoopId, String>> vertexIdPubId = resultVertices.map(new MapFunction<Vertex, Tuple2<GradoopId, String>>() {
            public Tuple2<GradoopId, String> map(Vertex in) {
                return Tuple2.of(in.getId(), in.getPropertyValue("recId").toString());
            }
        });
        gtRecorsNoSet = vertexIdPubId.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<GradoopId, String>, Tuple2<String, Long>>() {
            public void reduce(Iterable<Tuple2<GradoopId, String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
                Long cnt = 0l;
                for (Tuple2<GradoopId, String> v:values) {
                    cnt++;
                }
                out.collect(Tuple2.of("gt",(cnt*(cnt-1)/2)));
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of("gt",value1.f1+value2.f1);
            }
        });
        if(hasOverlap){
            resultVertices = resultVertices.flatMap(new FlatMapFunction<Vertex, Vertex>() {
                public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
                    String clusterids = vertex.getPropertyValue("ClusterId").toString();
                    if (clusterids.contains(",")) {
                        String[] clusteridsArray = clusterids.split(",");
                        for (int i=0;i<clusteridsArray.length;i++) {
                            vertex.setProperty("ClusterId",clusteridsArray[i]);
                            collector.collect(vertex);
                        }
                    }
                    else
                        collector.collect(vertex);
                }
            });
        }

        //////All Positives

        DataSet<Tuple2<String,String>> srcIdClusterId = resultVertices.map(new MapFunction<Vertex, Tuple2<String, String>>() {
            public Tuple2<String, String> map(Vertex in) {
                return Tuple2.of(in.getPropertyValue("type").toString(), in.getPropertyValue("ClusterId").toString());
            }
        });
        DataSet<Tuple3<String,String,Long>> srcIdClusterIdNo = srcIdClusterId.groupBy(0,1)
                .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Long>>() {
            public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple3<String, String, Long>> out) throws Exception {
                Long cnt = 0l;
                String s1,s2;
                s1=s2="";
                for(Tuple2<String, String> i:in) {
                    cnt++;
                    s1=i.f0;
                    s2=i.f1;
                }
                out.collect(Tuple3.of(s1,s2,cnt));
            }
        });
        DataSet<Long> apsetTemp = srcIdClusterIdNo.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple3<String,String,Long>, Long>() {
            public void reduce(Iterable<Tuple3<String,String,Long>> in, Collector<Long> out) throws Exception {
                Long cnt = 0l;
                ArrayList<Long> list = new ArrayList<Long>();
                for(Tuple3<String,String,Long> i:in) {
                    list.add(i.f2);
                }
                Long[] listArray = list.toArray(new Long[list.size()]);
                for(int i=0; i<listArray.length; i++){
                    for(int j=i+1; j<listArray.length; j++) {
                        cnt+=listArray[i]*listArray[j];
                    }
                }
                out.collect(cnt);
            }
        }).reduce(new ReduceFunction<Long>() {
            public Long reduce(Long in1, Long in2) throws Exception {
                return in1+in2;
            }
        });
        apset = apsetTemp.map(new MapFunction<Long, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(Long in) throws Exception {
                return Tuple2.of("ap",in);
            }
        });

        ///////True Positives
        DataSet<Tuple3<String,String,String>> srcIdClusterIdPubId = resultVertices.map(new MapFunction<Vertex, Tuple3<String,String,String>>() {
            public Tuple3<String,String,String> map(Vertex in) {
                return Tuple3.of(in.getPropertyValue("type").toString(), in.getPropertyValue("ClusterId").toString(),
                        in.getPropertyValue("recId").toString());
            }
        });
        DataSet<Long> tpsetTemp = srcIdClusterIdPubId.groupBy(1,2).reduceGroup(new GroupReduceFunction<Tuple3<String, String, String>, Long>() {
            public void reduce(Iterable<Tuple3<String, String, String>> in, Collector<Long> out) throws Exception {
                long cnt = 0l;
                for (Tuple3<String, String, String> i:in)
                    cnt++;
                out.collect(cnt*(cnt-1)/2);
            }
        }).reduce(new ReduceFunction<Long>() {
            public Long reduce(Long in1, Long in2) throws Exception {
                return in1+in2;
            }
        });
        tpset = tpsetTemp.map(new MapFunction<Long, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(Long in) throws Exception {
                return Tuple2.of("tp",in);
            }
        });


    }

    private void computeValues() throws Exception {

        if (clusterNoSet == null)
            computeSets();

        DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(gtRecorsNoSet);
        for (Tuple2<String, Long> i : sets.collect()) {

            if (i.f0.equals("ap")) {
                ap = i.f1;
                System.out.println("ap "+ap);

            }
            else if (i.f0.equals("tp")) {
                tp = i.f1;
                System.out.println("tp "+tp);

            }
            else if (i.f0.equals("gt")) {
                gtRecorsNo = i.f1;
                System.out.println("gtRecorsNo "+gtRecorsNo);
            }

        }
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


}

